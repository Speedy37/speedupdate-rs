use futures::{Async, Poll, Stream};
use futures::task;
use futures::task::Task;
use operation::Operation;
use std::cmp;
use std::fmt;
use std::collections::VecDeque;
use std::fs::{remove_file, OpenOptions};
use std::io;
use std::io::prelude::*;
use std::sync::{Arc, Condvar, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use storage::v1;
use workspace::{UpdatePosition, WorkspaceFileManager};
use updater::UpdateOptions;

use BUFFER_SIZE;

type Item = Result<(usize, u64, u64), ApplyError>;

#[derive(Debug)]
pub enum ApplyError {
  OperationFailed((String, io::Error)),
  DownloadAbort,
  PoisonError,
}

impl fmt::Display for ApplyError {
  fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
    match self {
      &ApplyError::OperationFailed((ref path, ref err)) => {
        write!(fmt, "operation failed {}", path.as_str())?;
        err.fmt(fmt)
      }
      &ApplyError::DownloadAbort => write!(fmt, "download abort"),
      &ApplyError::PoisonError => write!(fmt, "mutex poison error"),
    }
  }
}

#[derive(Debug)]
enum InternalApplyError {
  IoError(io::Error),
  DownloadAbort,
  PoisonError,
}

impl From<io::Error> for InternalApplyError {
  fn from(io_error: io::Error) -> InternalApplyError {
    InternalApplyError::IoError(io_error)
  }
}

fn wait_until<T, F>(
  condition: &(Mutex<(bool, T)>, Condvar),
  until: F,
) -> Result<T, InternalApplyError>
where
  T: Clone,
  F: Fn(&T) -> bool,
{
  use self::InternalApplyError::*;
  let &(ref lock, ref cvar) = condition;
  let mut guard = lock.lock();
  loop {
    match guard {
      Ok(res) => {
        {
          let &(abort, ref data) = &*res;
          if abort {
            return Err(DownloadAbort);
          }
          if until(data) {
            return Ok(data.clone());
          }
        }
        guard = cvar.wait(res);
      }
      Err(_) => return Err(PoisonError),
    }
  }
}

pub fn notify(mutex: &Mutex<(VecDeque<Item>, Option<Task>)>, value: Item) {
  if let Ok(ref mut data) = mutex.lock() {
    let push = match (data.0.front_mut(), &value) {
      (Some(&mut Ok(ref mut cur_pos)), &Ok(ref new_pos)) => {
        cur_pos.0 = new_pos.0.clone();
        cur_pos.1 += new_pos.1;
        cur_pos.2 += new_pos.2;
        false
      }
      _ => true,
    };
    if push {
      data.0.push_back(value);
    }
    if let Some(ref task) = data.1 {
      task.notify();
    }
  }
}

pub fn notify_end(mutex: &Mutex<(VecDeque<Item>, Option<Task>)>) {
  if let Ok(ref mut data) = mutex.lock() {
    if let Some(ref task) = data.1 {
      task.notify();
    }
  }
}

pub struct ApplyStream {
  done: Arc<AtomicUsize>,
  i_available: Arc<(Mutex<(bool, UpdatePosition)>, Condvar)>,
  o_applied: Arc<Mutex<(VecDeque<Item>, Option<Task>)>>,
}

impl ApplyStream {
  pub fn notify(&self, value: UpdatePosition) {
    let &(ref lock, ref cvar) = &*self.i_available;
    let mut started = lock.lock().unwrap();
    (*started).1 = value;
    cvar.notify_one();
  }

  pub fn abort(&self) {
    let &(ref lock, ref cvar) = &*self.i_available;
    let mut started = lock.lock().unwrap();
    (*started).0 = true;
    cvar.notify_one();
  }
}
impl Stream for ApplyStream {
  type Item = (usize, u64, u64);
  type Error = ApplyError;

  fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
    if let Ok(ref mut data) = self.o_applied.lock() {
      match data.0.pop_front() {
        Some(Ok(res)) => Ok(Async::Ready(Some(res))),
        Some(Err(err)) => Err(err),
        None => if self.done.compare_and_swap(1, 2, Ordering::Relaxed) > 0 {
          Ok(Async::Ready(None))
        } else {
          data.1 = Some(task::current());
          Ok(Async::NotReady)
        },
      }
    } else {
      Ok(Async::NotReady)
    }
  }
}

pub fn apply_package(
  update_options: UpdateOptions,
  file_manager: WorkspaceFileManager,
  operations: Vec<(usize, v1::Operation)>,
  i_available: Arc<(Mutex<(bool, UpdatePosition)>, Condvar)>,
) -> ApplyStream {
  let done = Arc::new(AtomicUsize::new(0));
  let o_applied = Arc::new(Mutex::new((VecDeque::new(), None)));
  let t_done = done.clone();
  let t_applied = o_applied.clone();
  let t_available = i_available.clone();
  thread::spawn(move || -> () {
    let terr_applied = t_applied.clone();
    let mut applied_data = UpdatePosition::new();
    let mut apply_operation =
      move |idx, operation: &v1::Operation| -> Result<(), InternalApplyError> {
        applied_data.package_idx = idx;
        applied_data.byte_idx = 0;
        let res = operation
          .begin_apply(&file_manager, idx, &update_options)
          .map_err(|err| {
            warn!("begin apply operation#{} {} failed: {}", idx, operation.path(), err);
            err
          })?;
        debug!("begin apply operation#{} {}", idx, operation.path());
        if let Some(mut applier) = res {
          let mut buffer = [0u8; BUFFER_SIZE];

          wait_until(&t_available, |available| idx <= available.package_idx)?;

          let data_file_path = file_manager.download_operation_path(idx);
          {
            let mut total_output_bytes = 0;
            let mut data_file = OpenOptions::new().read(true).open(&data_file_path).map_err(|err| {
              warn!("apply operation#{} {} failed: unable to open data file ({})", idx, operation.path(), err);
              err
            })?;
            let mut remaining = applier.data_size();
            while remaining > 0 {
              let available = wait_until(&t_available, |available| applied_data < *available)?;
              let available = if available.package_idx == applied_data.package_idx {
                available.byte_idx - applied_data.byte_idx
              } else {
                remaining
              };

              let max_read = cmp::min(available, buffer.len() as u64) as usize;
              let read = data_file.read(&mut buffer[0..max_read]).map_err(|err| {
                warn!("apply operation#{} {} failed: unable to read data file ({})", idx, operation.path(), err);
                err
              })?;
              applier.write_all(&buffer[0..read]).map_err(|err| {
                warn!("apply operation#{} {} failed: unable to write final file ({})", idx, operation.path(), err);
                err
              })?;
              let read = read as u64;
              applied_data.byte_idx += read;
              remaining -= read;

              let new_total_output_bytes = applier.written_bytes();
              let delta_output_bytes = new_total_output_bytes - total_output_bytes;
              notify(
                &t_applied,
                Ok((applied_data.package_idx, read, delta_output_bytes)),
              );
              total_output_bytes = new_total_output_bytes;
            }
            applier.commit().map_err(|err| {
              warn!("apply operation#{} {} failed: unable to commit changes ({})", idx, operation.path(), err);
              err
            })?;
          }
          remove_file(&data_file_path)?;
        }

        applied_data.package_idx += 1;
        applied_data.byte_idx = 0;
        notify(&t_applied, Ok((applied_data.package_idx, 0, 0)));
        Ok(())
      };
    for &(idx, ref operation) in operations.iter() {
      if let Err(err) = apply_operation(idx, operation) {
        let err = match err {
          InternalApplyError::IoError(io_err) => {
            ApplyError::OperationFailed((operation.path().to_owned(), io_err))
          }
          InternalApplyError::DownloadAbort => ApplyError::DownloadAbort,
          InternalApplyError::PoisonError => ApplyError::PoisonError,
        };
        notify(&terr_applied, Err(err));
      }
    }
    t_done.store(1, Ordering::Relaxed);
    notify_end(&terr_applied);
    debug!("end apply");
  });

  ApplyStream {
    done,
    o_applied,
    i_available,
  }
}
