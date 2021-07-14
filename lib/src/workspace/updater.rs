use std::cell::RefCell;
use std::mem;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{fmt, task::Poll};

use futures::future::Either;
use futures::prelude::*;
use tracing::{debug, error, info, warn};

use super::apply::{apply_package, ApplyError, ApplyStream, AvailableForApply};
use super::download::{download_package, DownloadStream};
use super::progress::{Progression, SharedUpdateProgress, UpdateStage};
use crate::link::{RemoteRepository, RepositoryError};
use crate::metadata::v1::{State, StateUpdating};
use crate::metadata::{self, Operation, Package};
use crate::workspace::{UpdatePosition, Workspace, WorkspaceFileManager};

#[derive(Debug)]
pub enum UpdateError {
    LocalStateError(std::io::Error),
    LocalCheckError(std::io::Error),
    LocalWorkspaceError(std::io::Error),
    Repository(RepositoryError),
    NoPath,
    Download(RepositoryError),
    DownloadCache(std::io::Error),
    Failed { files: usize },
    PoisonError,
}

impl fmt::Display for UpdateError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            UpdateError::LocalStateError(err) => write!(f, "local state.json error: {}", err),
            UpdateError::LocalCheckError(err) => write!(f, "local check.json error:: {}", err),
            UpdateError::LocalWorkspaceError(err) => write!(f, "local workspace error: {}", err),
            UpdateError::Repository(err) => write!(f, "repository error: {}", err),
            UpdateError::NoPath => write!(f, "repository error: no update path found"),
            UpdateError::Download(err) => write!(f, "download error: {}", err),
            UpdateError::DownloadCache(err) => write!(f, "download cache error: {}", err),
            UpdateError::Failed { files } => write!(f, "update failed for {} files", files),
            UpdateError::PoisonError => write!(f, "internal error: mutex poisonned"),
        }
    }
}

#[derive(Clone)]
pub struct UpdateOptions {
    /// If `true`, check existing files integrity.
    ///
    /// Default to `false`.
    pub check: bool,
    /// If `true`, some meta logic warning (rmdir on ue4pak) are converted to errors
    ///
    /// Default to `true`.
    pub strict_meta: bool,
    /// If `true`, some fs update warning (rmdir) are converted to errors
    ///
    /// Default to `false`.
    pub strict_fs: bool,
    /// Minimum duration to wait before saving updating state again
    ///
    /// Default to `5s`.
    pub save_state_interval: Duration,
}

impl Default for UpdateOptions {
    fn default() -> Self {
        Self {
            check: false,
            strict_meta: true,
            strict_fs: false,
            save_state_interval: Duration::from_secs(5),
        }
    }
}

struct UpdatePackageStream<'a> {
    state: Rc<RefCell<StateUpdating>>,
    shared_state: SharedUpdateProgress,
    download_stream: DownloadStream<'a>,
    apply_stream: ApplyStream,
}

impl<'a> UpdatePackageStream<'a> {
    fn new<R>(
        update_options: UpdateOptions,
        state: Rc<RefCell<StateUpdating>>,
        shared_state: SharedUpdateProgress,
        file_manager: WorkspaceFileManager,
        repository: &'a R,
        package_name: &metadata::CleanName,
        operations: Vec<(usize, Arc<metadata::v1::Operation>)>,
    ) -> Result<UpdatePackageStream<'a>, UpdateError>
    where
        R: RemoteRepository,
    {
        let (available, applied) = {
            let state = &*state.borrow();
            (state.available.clone(), state.applied.clone())
        };
        let download_operations: Vec<(usize, _)> = operations
            .iter()
            .skip_while(|&&(idx, _)| idx < available.operation_idx)
            .cloned()
            .collect();
        let apply_operations: Vec<(usize, _)> = operations
            .iter()
            .skip_while(|&&(idx, _)| idx < applied.operation_idx)
            .cloned()
            .collect();

        file_manager.create_update_dirs().map_err(UpdateError::LocalWorkspaceError)?;

        let i_available = AvailableForApply::new(available);
        let apply_stream = apply_package(
            update_options,
            file_manager.clone(),
            package_name,
            apply_operations,
            i_available.clone(),
        );
        let download_stream = download_package(
            file_manager,
            repository,
            package_name,
            download_operations,
            available.clone(),
        );

        Ok(UpdatePackageStream { state, shared_state, download_stream, apply_stream })
    }
}

impl<'a> Stream for UpdatePackageStream<'a> {
    type Item = Result<SharedUpdateProgress, UpdateError>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let download_poll = this.download_stream.poll_next_unpin(cx);
        let apply_poll = this.apply_stream.poll_next_unpin(cx);

        match (download_poll, apply_poll) {
            (Poll::Ready(None), Poll::Ready(None)) => Poll::Ready(None),
            (Poll::Pending, Poll::Pending) => Poll::Pending,
            (Poll::Pending, Poll::Ready(None)) => Poll::Pending,
            (Poll::Ready(None), Poll::Pending) => Poll::Pending,
            (Poll::Ready(Some(Err(err))), _) => {
                // Download errors cause the apply thread to be cancelled
                this.apply_stream.cancel();
                Poll::Ready(Some(Err(err)))
            }
            (download_poll, apply_poll) => {
                let mut delta = Progression::default();
                if let Poll::Ready(Some(Ok(download_progress))) = download_poll {
                    this.state.borrow_mut().available = download_progress.available.clone();

                    let mut state = this.shared_state.borrow_mut();
                    state.downloading_operation_idx = download_progress.available.operation_idx;
                    delta.downloaded_files = download_progress.delta_downloaded_files;
                    delta.downloaded_bytes = download_progress.delta_downloaded_bytes;
                    this.apply_stream.notify(download_progress.available);
                }
                if let Poll::Ready(Some(apply_progress)) = apply_poll {
                    match apply_progress {
                        Ok(apply_progress) => {
                            this.state.borrow_mut().applied.operation_idx =
                                apply_progress.operation_idx;
                            let mut state = this.shared_state.borrow_mut();
                            state.applying_operation_idx = apply_progress.operation_idx;
                            delta.applied_files = apply_progress.delta_applied_files;
                            delta.applied_input_bytes = apply_progress.delta_input_bytes;
                            delta.applied_output_bytes = apply_progress.delta_output_bytes;
                        }
                        Err(ApplyError::OperationFailed { path, slice, cause }) => {
                            warn!("{} failed: {}", path, cause);
                            let mut state = this.state.borrow_mut();
                            state.failures.push(match slice {
                                Some(slice) => metadata::v1::Failure::Slice { path, slice },
                                None => metadata::v1::Failure::Path { path },
                            });
                            delta.failed_files = 1;
                        }
                        Err(ApplyError::Cancelled) => {}
                        Err(ApplyError::PoisonError) => {
                            return Poll::Ready(Some(Err(UpdateError::PoisonError)))
                        }
                    }
                }

                {
                    let mut state = this.shared_state.borrow_mut();
                    state.inc_progress(delta);
                }

                Poll::Ready(Some(Ok(this.shared_state.clone())))
            }
        }
    }
}

pub(super) struct UpdateFilter {
    failures: Vec<metadata::v1::Failure>,
}

impl UpdateFilter {
    fn allows_all() -> Self {
        Self { failures: Vec::new() }
    }

    pub(super) fn filter(&self, o: &metadata::v1::Operation) -> bool {
        self.failures.is_empty()
            || self.failures.binary_search_by_key(&o.path(), |f| f.path()).is_ok()
    }

    fn filter_map(&self, o: &metadata::v1::Operation) -> Option<metadata::v1::Operation> {
        if self.failures.is_empty()
            || self
                .failures
                .binary_search_by_key(&(o.path(), o.slice()), |f| (f.path(), f.slice()))
                .is_ok()
        {
            Some(o.clone())
        } else if o.slice().is_some()
            && self.failures.binary_search_by_key(&o.path(), |f| f.path()).is_ok()
        {
            o.as_check_operation()
        } else {
            None
        }
    }
}

pub type GlobalProgressStream<'a> =
    Pin<Box<dyn Stream<Item = Result<SharedUpdateProgress, UpdateError>> + 'a>>;

// get -> stream of bytes -> write -> progression
// progression -> apply -> progression

pub(crate) async fn update<'a, R>(
    workspace: &'a mut Workspace,
    repository: &'a R,
    goal_version: Option<metadata::CleanName>,
    update_options: UpdateOptions,
) -> Result<impl Stream<Item = Result<SharedUpdateProgress, UpdateError>> + 'a, UpdateError>
where
    R: RemoteRepository,
{
    let goal_version = if let Some(goal_version) = goal_version {
        goal_version.to_owned()
    } else {
        let current_version = repository.current_version().map_err(UpdateError::Repository).await?;
        current_version.version().clone()
    };
    info!("update to {}", goal_version);

    // Load current workspace state
    workspace.file_manager().create_update_dirs().map_err(UpdateError::LocalWorkspaceError)?;

    if let Err(err) = workspace.reload_state_from_fs() {
        warn!("unable to load current workspace state: {}", err);
    };

    if let State::Stable { version } = workspace.state() {
        if version == &goal_version && !update_options.check {
            // Everything is uptodate, and nothing requires fixing
            return Ok(Either::Right(stream::empty()));
        }
    }

    let mut workspace_state = workspace.state().clone();
    let failures = match &mut workspace_state {
        State::Corrupted { failures, .. } => std::mem::take(failures),
        State::Updating(state) => {
            state.dedup_failures();
            std::mem::take(&mut state.failures)
        }
        _ => Vec::new(),
    };

    let shared_state_n =
        Rc::new(RefCell::new(StateUpdating::new(None, goal_version.clone(), failures)));
    let shared_state_r = shared_state_n.clone();
    let shared_state_s = shared_state_n.clone();
    let shared_state_c = shared_state_n.clone();

    let global_progression_n = SharedUpdateProgress::new(goal_version.clone());
    let global_progression_r = global_progression_n.clone();
    let global_progression_nr = global_progression_n.clone();
    let global_progression_c = global_progression_n.clone();

    let file_manager_n = workspace.file_manager();
    let file_manager_r = file_manager_n.clone();

    let goal_version_n = goal_version.clone();
    let goal_version_r = goal_version.clone();

    let update_options_r = update_options.clone();
    let update_options_s = update_options.clone();

    let write_state_nr = Rc::new(RefCell::new(move || -> Result<(), UpdateError> {
        let state = &*shared_state_s.borrow();
        if state.check_only {
            return Ok(());
        }
        let global_progression = &*global_progression_nr.borrow();
        let state = if state.failures.len() == 0
            && global_progression.applying_package_idx == global_progression.steps.len()
        {
            State::Stable { version: state.to.clone() }
        } else {
            State::Updating(state.clone())
        };
        workspace.set_state(state).map_err(UpdateError::LocalStateError)?;
        Ok(())
    }));
    let write_state_c = write_state_nr.clone();

    // 1. try to the update normally
    let normal_stream = update_internal(
        update_options,
        file_manager_n,
        global_progression_n,
        workspace_state,
        shared_state_n,
        repository,
        goal_version_n,
        UpdateFilter::allows_all(),
        UpdateStage::Updating,
    )
    .try_flatten_stream();

    // 2. try to repair update errors
    let repair_stream = future::lazy(move |_| {
        let mut failures = {
            let state = &mut *shared_state_r.borrow_mut();
            state.previous_failures = mem::take(&mut state.failures);
            state.previous_failures.clone()
        };
        if failures.len() > 0 {
            failures.sort();
            global_progression_r.borrow_mut().stage = UpdateStage::FindingRepairPath;
            Either::Left(
                update_internal(
                    update_options_r,
                    file_manager_r,
                    global_progression_r,
                    State::New, //< force to repair from no starting revision
                    shared_state_r,
                    repository,
                    goal_version_r,
                    UpdateFilter { failures },
                    UpdateStage::Repairing,
                )
                .try_flatten_stream(),
            )
        } else {
            // nothing to repair, everything went well
            Either::Right(stream::empty())
        }
    })
    .flatten_stream();

    let commit_stream = future::lazy(move |_| {
        if let Err(err) = (&mut *write_state_c.borrow_mut())() {
            // Failed to write state
            return Either::Right(stream::once(async { Err(err) }));
        }

        let state = &mut *shared_state_c.borrow_mut();
        state.previous_failures = Vec::new();
        let last_res = if state.failures.len() == 0 {
            info!("update to {} succeeded", goal_version);
            global_progression_c.borrow_mut().stage = UpdateStage::Uptodate;
            Ok(global_progression_c.clone())
        } else {
            error!("update to {} failed", goal_version);
            global_progression_c.borrow_mut().stage = UpdateStage::Failed;
            let err = UpdateError::Failed { files: state.failures.len() };
            Err(err)
        };
        Either::Left(stream::once(async { last_res }))
    })
    .flatten_stream();

    let mut last_write = Instant::now();
    let final_stream = normal_stream
        .chain(repair_stream)
        .inspect(move |_| {
            let now = Instant::now();
            if now.duration_since(last_write) > update_options_s.save_state_interval {
                let _ignore_err = (&mut *write_state_nr.borrow_mut())();
                last_write = now;
            }
        })
        .chain(commit_stream);

    Ok(Either::Left(final_stream))
}

async fn update_path<R>(
    initial_state: State,
    repository: &R,
    goal_version: &metadata::CleanName,
    check: bool,
) -> Result<Option<(Vec<Arc<metadata::PackageMetadata>>, StateUpdating)>, UpdateError>
where
    R: RemoteRepository,
{
    let packages = repository.packages().map_err(UpdateError::Repository).await?;
    let maybe_path = shortest_path(initial_state, packages.as_slice(), goal_version, check)?;
    let (path, first_package_state) = match maybe_path {
        Some(x) => x,
        None => return Ok(None),
    };

    let package_names =
        path.iter().map(|package| package.package_metadata_name()).collect::<Vec<_>>();

    info!("found update path {:?}", package_names);

    let packages_metadata = stream::iter(package_names.into_iter())
        .map(|package_name| repository.package_metadata(package_name))
        .buffered(4)
        .map_ok(Arc::new)
        .try_collect()
        .map_err(UpdateError::Repository)
        .await?;

    Ok(Some((packages_metadata, first_package_state)))
}

async fn update_internal<'a, R>(
    update_options: UpdateOptions,
    file_manager: WorkspaceFileManager,
    global_progression: SharedUpdateProgress,
    initial_state: State,
    shared_state: Rc<RefCell<StateUpdating>>,
    repository: &'a R,
    goal_version: metadata::CleanName,
    filter: UpdateFilter,
    main_stage: UpdateStage,
) -> Result<impl Stream<Item = Result<SharedUpdateProgress, UpdateError>> + 'a, UpdateError>
where
    R: RemoteRepository,
{
    let maybe_path =
        update_path(initial_state, repository, &goal_version, update_options.check).await?;
    let packages_metadata = match maybe_path {
        Some((packages_metadata, first_package_state)) => {
            // Update global progress with objectives
            global_progression.borrow_mut().push_steps(
                &packages_metadata,
                &first_package_state,
                &filter,
            );

            // Setup shared workspace state
            shared_state.borrow_mut().update_with(first_package_state);

            packages_metadata
        }
        None => Vec::new(),
    };

    {
        global_progression.borrow_mut().stage = main_stage;
    }

    let state_p = shared_state.clone();

    let update_package_stream = packages_metadata.into_iter().map(move |package_metadata| {
        // Update workspace updating state details
        let check_only = {
            let state = &mut *state_p.borrow_mut();
            state.from = package_metadata.from().cloned();
            state.to = package_metadata.to().clone();
            debug!(
                "begin {} package = {}, available = {:?}, applied = {:?}",
                if state.check_only { "check" } else { "update" },
                package_metadata.package_data_name(),
                state.available,
                state.applied
            );
            state.check_only
        };

        // Build list of operations to do
        let operations: Vec<(usize, Arc<metadata::v1::Operation>)> = package_metadata
            .iter()
            .enumerate()
            .filter_map(|(idx, o)| {
                let maybe_o = if !check_only {
                    filter.filter_map(o).map(|o| (idx, Arc::new(o)))
                } else {
                    None
                };
                if maybe_o.is_none() && update_options.check {
                    o.as_check_operation().map(|o| (idx, Arc::new(o)))
                } else {
                    maybe_o
                }
            })
            .collect();

        // Write package check file
        {
            let check_operations: Vec<metadata::v1::Operation> =
                package_metadata.iter().filter_map(|o| o.as_check_operation()).collect();
            let checks = metadata::WorkspaceChecks::V1 { operations: check_operations };
            file_manager.write_checks(&checks).map_err(UpdateError::LocalCheckError)?;
        }

        // Build downloader & applier stream
        let normal_stream = UpdatePackageStream::new(
            update_options.clone(),
            state_p.clone(),
            global_progression.clone(),
            file_manager.clone(),
            repository,
            &package_metadata.package_data_name(),
            operations,
        )?;

        let state_c = state_p.clone();
        let global_progression_c = global_progression.clone();
        let commit_stream = future::lazy(move |_| {
            debug!("end update package");
            let mut state = &mut *state_c.borrow_mut();
            state.available = UpdatePosition::new();
            state.applied = UpdatePosition::new();
            global_progression_c.borrow_mut().inc_package();
            stream::empty()
        })
        .flatten_stream();

        Ok(normal_stream.chain(commit_stream))
    });

    let update_stream = stream::iter(update_package_stream).try_flatten();

    Ok(update_stream)
}

fn shortest_path<'a, P>(
    working_state: State,
    packages: &'a [P],
    goal_version: &metadata::CleanName,
    check: bool,
) -> Result<Option<(Vec<&'a P>, StateUpdating)>, UpdateError>
where
    P: Package,
{
    let mut path = Vec::new();
    let (start, maybe_state) = match working_state {
        State::New => (None, None),
        State::Stable { version } => (Some(version), None),
        State::Corrupted { version, .. } => (Some(version), None),
        State::Updating(state) => {
            match packages
                .iter()
                .find(|&package| package.from() == state.from.as_ref() && package.to() == &state.to)
            {
                Some(package) => {
                    path.push(package);
                    (Some(state.to.clone()), Some(state))
                }
                _ => (None, None), // package doesn't exist anymore, assume no current version
            }
        }
    };
    if start.as_ref() != Some(goal_version) {
        match metadata::shortest_path(start.as_ref(), goal_version, &packages) {
            Some(ref mut npath) => path.append(npath),
            _ => return Err(UpdateError::NoPath),
        }
    }
    if path.len() > 0 {
        let p0 = path[0];
        let from = p0.from().cloned();
        let to = p0.to().clone();
        let state = match maybe_state {
            Some(mut state) => {
                state.from = from;
                state.to = to;
                state
            }
            None => StateUpdating::new(from, to, Vec::new()),
        };
        Ok(Some((path, state)))
    } else if check {
        match packages.iter().find(|p| p.to() == goal_version) {
            Some(p0) => {
                let path = vec![p0];
                let mut state = StateUpdating::new(
                    Some(goal_version.clone()),
                    goal_version.clone(),
                    Vec::new(),
                );
                state.check_only = true;
                Ok(Some((path, state)))
            }
            None => Err(UpdateError::NoPath),
        }
    } else {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AutoRepository;

    #[test]
    fn update_ret_size() {
        fn size_of_fn4_ret<F, R, A, B, C, D>(_f: F) -> usize
        where
            F: FnOnce(A, B, C, D) -> R,
        {
            std::mem::size_of::<F::Output>()
        }
        let update_ret_size = size_of_fn4_ret(update::<AutoRepository>);
        assert!(update_ret_size < 256, "update_ret_size = {} < 128", update_ret_size);
    }
}
