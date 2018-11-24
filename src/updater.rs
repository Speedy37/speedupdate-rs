use apply::{apply_package, ApplyError, ApplyStream};
use download::download_package;
use futures::{future, stream, Async, Future, Poll, Stream};
use operation::Operation;
use progression::{GlobalProgression, Progression};
use repository::{Error as RepositoryError, RemoteRepository};
use serde_json;
use std::collections::HashSet;
use std::fmt;
use std::fs;
use std::io;
use std::mem;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};
use storage;
use storage::{Package, v1};
use workspace::{CheckPackageMetadata, State, StateUpdating, UpdatePosition, Workspace,
                WorkspaceFileManager};

#[derive(Debug)]
pub enum Error {
  RemoteRepository(RepositoryError),
  IoError(io::Error),
  NoPath,
  RecoveryFailed,
  ApplyError(ApplyError),
}

#[derive(Clone)]
pub struct UpdateOptions {
  pub check: bool,
}

struct ProgressionStream<'a> {
  state: Rc<RefCell<StateUpdating>>,
  download_stream: stream::Fuse<Box<Stream<Item = (UpdatePosition, u64), Error = Error> + 'a>>,
  apply_stream: ApplyStream,
  last_download_operation_index: usize,
  last_apply_operation_index: usize,
}

impl<'a> ProgressionStream<'a> {
  fn new<R>(
    update_options: UpdateOptions,
    state: Rc<RefCell<StateUpdating>>,
    file_manager: WorkspaceFileManager,
    repository: &'a R,
    package_name: String,
    operations: Vec<(usize, v1::Operation)>,
  ) -> Result<ProgressionStream<'a>, Error>
  where
    R: RemoteRepository,
  {
    let (available, applied) = {
      let state = &*state.borrow();
      (state.available.clone(), state.applied.clone())
    };
    let download_operations: Vec<(usize, _)> = operations
      .iter()
      .skip_while(|&&(idx, _)| idx < available.package_idx)
      .cloned()
      .collect();
    let apply_operations: Vec<(usize, _)> = operations
      .iter()
      .skip_while(|&&(idx, _)| idx < applied.package_idx)
      .cloned()
      .collect();

    file_manager.remove_tmp_dir()?;
    file_manager.create_update_dirs()?;

    let i_available = Arc::new((Mutex::new((false, available.clone())), Condvar::new()));
    let apply_stream = apply_package(
      update_options,
      file_manager.clone(),
      apply_operations,
      i_available.clone(),
    );
    let download_stream = download_package(
      file_manager,
      repository,
      package_name,
      download_operations,
      available.clone(),
    ).fuse();

    Ok(ProgressionStream {
      state,
      download_stream,
      apply_stream,
      last_download_operation_index: 0,
      last_apply_operation_index: 0,
    })
  }
}

impl<'a> Stream for ProgressionStream<'a> {
  type Item = Progression;
  type Error = Error;

  fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
    let download_poll = match self.download_stream.poll() {
      Ok(v) => v,
      Err(err) => {
        self.apply_stream.abort();
        return Err(err);
      }
    };

    let apply_poll = self.apply_stream.poll().map_err(Error::ApplyError)?;
    match (&download_poll, &apply_poll) {
      (&Async::Ready(None), &Async::Ready(None)) => Ok(None.into()),
      (&Async::NotReady, &Async::NotReady) => Ok(Async::NotReady),
      (&Async::NotReady, &Async::Ready(None)) => Ok(Async::NotReady),
      (&Async::Ready(None), &Async::NotReady) => Ok(Async::NotReady),
      _ => {
        let mut progression = Progression::new();
        if let &Async::Ready(Some((ref download_progress, bytes))) = &download_poll {
          (&mut *self.state.borrow_mut()).available = download_progress.clone();
          progression.downloaded_files =
            download_progress.package_idx - self.last_download_operation_index;
          self.last_download_operation_index = download_progress.package_idx;
          progression.downloaded_bytes = bytes;
          self.apply_stream.notify(download_progress.clone());
        }
        if let &Async::Ready(Some((apply_operation_index, delta_input_bytes, delta_output))) =
          &apply_poll
        {
          (&mut *self.state.borrow_mut()).applied.package_idx = apply_operation_index;
          progression.applied_files = apply_operation_index - self.last_apply_operation_index;
          self.last_apply_operation_index = apply_operation_index;
          progression.applied_input_bytes = delta_input_bytes;
          progression.applied_output_bytes = delta_output;
        }
        Ok(Some(progression).into())
      }
    }
  }
}

impl From<io::Error> for Error {
  fn from(err: io::Error) -> Error {
    Error::IoError(err)
  }
}

impl fmt::Display for Error {
  fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
    match self {
      &Error::NoPath => write!(fmt, "no update path"),
      &Error::RecoveryFailed => write!(fmt, "recovery failed"),
      &Error::IoError(ref err) => err.fmt(fmt),
      &Error::RemoteRepository(ref err) => err.fmt(fmt),
      &Error::ApplyError(ref err) => err.fmt(fmt),
    }
  }
}

use std::cell::RefCell;
use std::rc::Rc;
type GlobalProgressionStream<'a> =
  Box<Stream<Item = Rc<RefCell<GlobalProgression>>, Error = Error> + 'a>;

pub fn update<'a, R>(
  workspace: &'a mut Workspace,
  repository: &'a R,
  goal_version: &'a str,
  update_options: UpdateOptions,
) -> GlobalProgressionStream<'a>
where
  R: RemoteRepository,
{
  info!("update to {}", goal_version);
  let workspace_state = workspace.state().clone();
  if let State::Stable { ref version } = workspace_state {
    if version == goal_version {
      return Box::new(stream::empty());
    }
  }
  let global_progression = Rc::new(RefCell::new(GlobalProgression::new()));
  let shared_state = Rc::new(RefCell::new(StateUpdating::new(
    String::new(),
    String::new(),
  )));
  let shared_state_r = shared_state.clone();
  let shared_state_s = shared_state.clone();
  let shared_state_c = shared_state.clone();
  let global_progression_r = global_progression.clone();
  let global_progression_c = global_progression.clone();
  let file_manager = workspace.file_manager();
  let file_manager_r = workspace.file_manager();
  let update_options_r = update_options.clone();
  let normal_stream = update_internal(
    update_options,
    file_manager,
    global_progression,
    workspace_state,
    shared_state,
    repository,
    goal_version,
    |_| true,
  );
  let repair_stream = future::lazy(move || -> Result<GlobalProgressionStream<'a>, Error> {
    let set: HashSet<String> = {
      let state = &mut *shared_state_r.borrow_mut();
      mem::replace(&mut state.failures, Vec::new())
        .into_iter()
        .collect()
    };
    if set.len() > 0 {
      Ok(update_internal(
        update_options_r,
        file_manager_r,
        global_progression_r,
        State::New,
        shared_state_r,
        repository,
        goal_version,
        move |path| set.contains(path),
      ))
    } else {
      Ok(Box::new(stream::empty()))
    }
  }).flatten_stream();

  let write_state = Rc::new(RefCell::new(move || -> Result<(), Error> {
    let state = &*shared_state_s.borrow();
    let global_progression = &*global_progression_c.borrow();
    let state = if state.failures.len() == 0
      && global_progression.packages.start == global_progression.packages.end
    {
      State::Stable {
        version: state.to.clone(),
      }
    } else {
      State::Updating(state.clone())
    };
    workspace.set_state(state)?;
    Ok(())
  }));
  let write_state_c = write_state.clone();
  let commit_stream = future::lazy(
    move || -> Result<stream::Empty<Rc<RefCell<GlobalProgression>>, Error>, Error> {
      (&mut *write_state_c.borrow_mut())()?;
      let state = &*shared_state_c.borrow();
      if state.failures.len() == 0 {
        info!("update to {} succeeded", goal_version);
        Ok(stream::empty())
      }
      else  {
        error!("update to {} failed", goal_version);
        Err(Error::RecoveryFailed)
      }
    },
  ).flatten_stream();

  let mut last_write = Instant::now();
  let final_stream = normal_stream
    .chain(repair_stream)
    .then(move |res| {
      let now = Instant::now();
      if now.duration_since(last_write) > Duration::new(1, 0) {
        (&mut *write_state.borrow_mut())()?;
        last_write = now;
      }
      res
    })
    .chain(commit_stream);

  Box::new(final_stream)
}

fn update_internal<'a, R, F>(
  update_options: UpdateOptions,
  file_manager: WorkspaceFileManager,
  global_progression: Rc<RefCell<GlobalProgression>>,
  initial_state: State,
  shared_state: Rc<RefCell<StateUpdating>>,
  repository: &'a R,
  goal_version: &'a str,
  filter: F,
) -> GlobalProgressionStream<'a>
where
  R: RemoteRepository,
  F: 'a + Fn(&str) -> bool,
{
  let stream = repository
    .packages()
    .map_err(Error::RemoteRepository)
    .and_then(move |packages| {
      let (path, first_package_state) =
        shortest_path(initial_state, packages.as_slice(), goal_version)?;

      info!(
        "found update path {:?}",
        path
          .iter()
          .map(|package| package.package_metadata_name())
          .collect::<Vec<_>>()
      );
      let futures: Vec<_> = path
        .iter()
        .map(move |package| repository.package_metadata(&package.package_metadata_name()))
        .collect();
      // TODO: find out why collect is required (ie. check join_all lifetime ellision, ...)
      Ok(
        future::join_all(futures)
          .map_err(Error::RemoteRepository)
          .map(|packages_metadata| (packages_metadata, first_package_state)),
      )
    })
    .flatten()
    .and_then(move |(packages_metadata, first_package_state)| {
      {
        let (mut available, mut applied) = (
          first_package_state.available.clone(),
          first_package_state.applied.clone(),
        );
        let global_progression = &mut *global_progression.borrow_mut();
        for package_metadata in packages_metadata.iter() {
          global_progression.packages.end += 1;
          global_progression.downloaded_files.start += applied.package_idx;
          global_progression.applied_files.start += available.package_idx;
          for (idx, operation) in package_metadata.iter().enumerate() {
            let output_len = if idx < applied.package_idx {
              (operation.data_size(), operation.final_size())
            } else {
              (0, 0)
            };
            global_progression.downloaded_files.end += 1;
            global_progression.downloaded_bytes.start += match idx {
              idx if idx < available.package_idx => operation.data_size(),
              idx if idx == available.package_idx => available.byte_idx,
              _ => 0,
            };
            global_progression.downloaded_bytes.end += operation.data_size();
            global_progression.applied_files.end += 1;
            global_progression.applied_input_bytes.start += output_len.0;
            global_progression.applied_input_bytes.end += operation.data_size();
            global_progression.applied_output_bytes.start += output_len.1;
            global_progression.applied_output_bytes.end += operation.final_size();
          }
          available = UpdatePosition::new();
          applied = UpdatePosition::new();
        }
      }

      *shared_state.borrow_mut() = first_package_state;
      let u_state = shared_state.clone();
      let n_state = shared_state.clone();
      let updates = packages_metadata.into_iter().map(move |package_metadata| {
        {
          let state = &mut *u_state.borrow_mut();
          state.from = package_metadata.from().to_owned();
          state.to = package_metadata.to().to_owned();
          debug!(
            "begin update package = {}, available = {:?}, applied = {:?}",
            package_metadata.package_data_name(),
            state.available,
            state.applied
          );
        }
        let operations: Vec<(usize, v1::Operation)> = package_metadata
          .iter()
          .enumerate()
          .filter_map(|(idx, o)| {
            if filter(o.path()) {
              Some((idx, o.clone()))
            } else if update_options.check {
              o.as_check_operation().map(|o| (idx, o))
            } else {
              None
            }
          })
          .collect();

        // Write package check file
        {
          file_manager.create_update_dirs()?;
          let check_file = fs::File::create(file_manager.check_path())?;
          let check_operations: Vec<v1::Operation> = package_metadata
            .iter()
            .filter_map(|o| o.as_check_operation())
            .collect();
          let check_package_metadata = CheckPackageMetadata::V1 {
            operations: check_operations,
          };
          serde_json::to_writer_pretty(check_file, &check_package_metadata)
            .map_err(io::Error::from)?;
        }

        ProgressionStream::new(
          update_options.clone(),
          u_state.clone(),
          file_manager.clone(),
          repository,
          package_metadata.package_data_name(),
          operations,
        ).map(|s| {
          s.and_then(move |res| Ok(Some(res)))
            .chain(stream::once(Ok(None)))
        })
      });
      let normal_stream = stream::iter_result(updates).flatten().then(move |res| {
        match res {
          Ok(Some(progression)) => *global_progression.borrow_mut() += &progression,
          Ok(None) => {
            debug!("end update package");
            let mut state = &mut *n_state.borrow_mut();
            state.available = UpdatePosition::new();
            state.applied = UpdatePosition::new();
            let global_progression = &mut *global_progression.borrow_mut();
            global_progression.packages.start += 1;
          }
          Err(Error::ApplyError(ApplyError::OperationFailed((path, _)))) => {
            // operation failed, can be recovered
            (&mut *n_state.borrow_mut()).failures.push(path);
            let global_progression = &mut *global_progression.borrow_mut();
            global_progression.failed_files += 1;
          }
          Err(err) => {
            error!("update failed: {}", err);
            // update failed
            return Err(err);
          }
        };
        Ok(global_progression.clone())
      });
      Ok(normal_stream)
    })
    .flatten_stream();
  Box::new(stream)
}

fn shortest_path<'a, P>(
  working_state: State,
  packages: &'a [P],
  goal_version: &str,
) -> Result<(Vec<&'a P>, StateUpdating), Error>
where
  P: Package,
{
  let mut path = Vec::new();
  let mut state = match working_state {
    State::New => StateUpdating::new(String::new(), String::new()),
    State::Stable { version } => StateUpdating::new(String::new(), version),
    State::Updating(state) => {
      let mut ret = state.clone();
      match packages
        .iter()
        .find(|&package| package.from() == &state.from && package.to() == &state.to)
      {
        Some(package) => path.push(package),
        _ => ret.to = String::new(), // package doesn't exist anymore
      }
      ret
    }
  };
  if state.to != goal_version {
    match storage::shortest_path(&state.to, goal_version, &packages) {
      Some(ref mut npath) => path.append(npath),
      _ => return Err(Error::NoPath),
    }
  }
  if path.len() > 0 {
    let p0 = path[0];
    state.from = p0.from().to_string();
    state.to = p0.to().to_string();
  }
  Ok((path, state))
}
