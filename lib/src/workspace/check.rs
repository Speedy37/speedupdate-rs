use std::cell::RefCell;
use std::fmt;
use std::mem;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;

use futures::prelude::*;
use tracing::{debug, warn};

use super::apply::{apply_package, ApplyError, AvailableForApply};
use super::progress::{CheckProgression, SharedCheckProgress};
use super::UpdateOptions;
use super::{UpdatePosition, Workspace};
use crate::{io, metadata};

pub type GlobalCheckStream<'a> =
    Pin<Box<dyn Stream<Item = Result<SharedCheckProgress, CheckError>> + 'a>>;

#[derive(Debug)]
pub enum CheckError {
    NewWorkspace,
    LocalStateError(io::Error),
    LocalCheckError(io::Error),
    LocalWorkspaceError(io::Error),
    Failed { files: usize },
    PoisonError,
}

impl fmt::Display for CheckError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CheckError::NewWorkspace => write!(f, "cannot check on new workspaces"),
            CheckError::LocalStateError(err) => write!(f, "local state.json error: {}", err),
            CheckError::LocalCheckError(err) => write!(f, "local check.json error:: {}", err),
            CheckError::LocalWorkspaceError(err) => write!(f, "local workspace error: {}", err),
            CheckError::Failed { files } => write!(f, "check failed for {} files", files),
            CheckError::PoisonError => write!(f, "internal error: mutex poisonned"),
        }
    }
}

pub(crate) async fn check<'a>(
    workspace: &mut Workspace,
) -> Result<impl Stream<Item = Result<SharedCheckProgress, CheckError>> + '_, CheckError> {
    if matches!(workspace.state(), metadata::v1::State::New) {
        return Err(CheckError::NewWorkspace);
    }

    let file_manager = workspace.file_manager();
    let checks = file_manager.read_checks().map_err(CheckError::LocalCheckError)?;

    // Build list of operations to do
    let operations: Vec<(usize, Arc<metadata::v1::Operation>)> = checks
        .iter()
        .enumerate()
        .filter_map(|(idx, o)| o.as_check_operation().map(|o| (idx, Arc::new(o))))
        .collect();
    let global_progression_n = SharedCheckProgress::new(Arc::new(checks));
    let global_progression_c = global_progression_n.clone();
    let package_name = metadata::CleanName::from_static_str("local");
    let i_available =
        AvailableForApply::new(UpdatePosition { operation_idx: operations.len(), byte_idx: 0 });
    let failures_n: Rc<RefCell<Vec<metadata::v1::Failure>>> = Default::default();
    let failures_c = failures_n.clone();
    let check_stream = apply_package(
        UpdateOptions { check: true, ..UpdateOptions::default() },
        file_manager,
        &package_name,
        operations,
        i_available,
    )
    .map(move |res| {
        let mut delta = CheckProgression::default();
        match res {
            Ok(apply_progress) => {
                delta.checked_files = apply_progress.delta_applied_files;
                delta.checked_bytes = apply_progress.delta_input_bytes;
            }
            Err(ApplyError::OperationFailed { path, slice, cause }) => {
                warn!("{} failed: {}", path, cause);
                let failure = match slice {
                    Some(slice) => metadata::v1::Failure::Slice { path, slice },
                    None => metadata::v1::Failure::Path { path },
                };
                failures_n.borrow_mut().push(failure);
            }
            Err(ApplyError::Cancelled) => {}
            Err(ApplyError::PoisonError) => return Err(CheckError::PoisonError),
        }
        global_progression_n.borrow_mut().histogram.inc(delta);
        Ok(global_progression_n.clone())
    });

    let commit_stream = future::lazy(move |_| {
        debug!("end check package");
        let failures = mem::take(&mut *failures_c.borrow_mut());
        let state = workspace.state_mut();
        let res = match state {
            metadata::v1::State::Stable { version } if !failures.is_empty() => {
                *state = metadata::v1::State::Corrupted { version: version.clone(), failures };
                workspace.write_state()
            }
            metadata::v1::State::Updating(state) => {
                state.failures = failures;
                workspace.write_state()
            }
            _ => Ok(()),
        };
        let res = match res {
            Ok(()) => Ok(global_progression_c.clone()),
            Err(err) => Err(CheckError::LocalStateError(err)),
        };
        stream::once(async { res })
    })
    .flatten_stream();

    Ok(check_stream.chain(commit_stream))
}
