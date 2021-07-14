use std::collections::VecDeque;
use std::fmt;
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::task::{Context, Poll};
use std::thread;
use std::{cmp, pin::Pin};

use futures::{prelude::*, task::AtomicWaker};
use tracing::{debug, info, warn};

use super::updater::UpdateOptions;
use crate::handlers::{ApplyHandler, ApplyOperation, HandlerContext};
use crate::io;
use crate::metadata::{self, v1, Operation};
use crate::workspace::{UpdatePosition, WorkspaceFileManager};

type Item = Result<ApplyPackageProgression, ApplyError>;

#[derive(Debug)]
pub enum ApplyError {
    OperationFailed {
        path: metadata::CleanPath,
        slice: Option<metadata::CleanPath>,
        cause: std::io::Error,
    },
    Cancelled,
    PoisonError,
}

impl fmt::Display for ApplyError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ApplyError::OperationFailed { path, cause, slice: Some(slice) } => {
                write!(f, "operation {}#{} failed: {}", path, slice, cause)
            }
            ApplyError::OperationFailed { path, cause, slice: None } => {
                write!(f, "operation {} failed: {}", path, cause)
            }
            ApplyError::Cancelled => write!(f, "download abort"),
            ApplyError::PoisonError => write!(f, "mutex poison error"),
        }
    }
}

#[derive(Debug)]
enum InternalApplyError {
    IoError(io::Error),
    Cancelled,
    PoisonError,
}

impl From<io::Error> for InternalApplyError {
    fn from(io_error: io::Error) -> InternalApplyError {
        InternalApplyError::IoError(io_error)
    }
}

pub enum ApplyState {
    Continue,
    Cancel,
}

pub fn notify(mutex: &Mutex<(VecDeque<Item>, AtomicWaker)>, value: Item) {
    if let Ok(ref mut data) = mutex.lock() {
        let push = match (data.0.front_mut(), &value) {
            (Some(&mut Ok(ref mut cur_pos)), &Ok(ref new_pos)) => {
                cur_pos.operation_idx = new_pos.operation_idx;
                cur_pos.delta_input_bytes += new_pos.delta_input_bytes;
                cur_pos.delta_output_bytes += new_pos.delta_output_bytes;
                false
            }
            _ => true,
        };
        if push {
            data.0.push_back(value);
        }
        data.1.wake();
    }
}

pub fn notify_end(mutex: &Mutex<(VecDeque<Item>, AtomicWaker)>) {
    if let Ok(ref mut data) = mutex.lock() {
        data.1.wake();
    }
}

#[derive(Clone)]
pub struct AvailableForApply {
    shared: Arc<(Mutex<(ApplyState, UpdatePosition)>, Condvar)>,
}

impl AvailableForApply {
    pub(super) fn new(available: UpdatePosition) -> Self {
        Self { shared: Arc::new((Mutex::new((ApplyState::Continue, available)), Condvar::new())) }
    }

    fn wait_until<F>(&self, until: F) -> Result<UpdatePosition, InternalApplyError>
    where
        F: Fn(&UpdatePosition) -> bool,
    {
        let &(ref lock, ref cvar) = &*self.shared;
        let mut guard = lock.lock();
        loop {
            match guard {
                Ok(res) => {
                    let (state, data) = &*res;
                    if let ApplyState::Cancel = state {
                        return Err(InternalApplyError::Cancelled);
                    }
                    if until(data) {
                        return Ok(data.clone());
                    }
                    guard = cvar.wait(res);
                }
                Err(_) => return Err(InternalApplyError::PoisonError),
            }
        }
    }
}

pub struct ApplyStream {
    done: Arc<AtomicUsize>,
    i_available: AvailableForApply,
    o_applied: Arc<Mutex<(VecDeque<Item>, AtomicWaker)>>,
}

impl ApplyStream {
    pub(super) fn notify(&self, value: UpdatePosition) {
        let &(ref lock, ref cvar) = &*self.i_available.shared;
        let mut started = lock.lock().unwrap();
        (*started).1 = value;
        cvar.notify_one();
    }

    pub fn cancel(&self) {
        let &(ref lock, ref cvar) = &*self.i_available.shared;
        let mut started = lock.lock().unwrap();
        (*started).0 = ApplyState::Cancel;
        cvar.notify_one();
    }
}

impl Stream for ApplyStream {
    type Item = Result<ApplyPackageProgression, ApplyError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Ok(ref mut data) = self.o_applied.lock() {
            match data.0.pop_front() {
                Some(res) => Poll::Ready(Some(res)),
                None => {
                    if self
                        .done
                        .compare_exchange(1, 2, Ordering::Relaxed, Ordering::Relaxed)
                        .unwrap_or_else(|p| p)
                        > 0
                    {
                        Poll::Ready(None)
                    } else {
                        data.1.register(cx.waker());
                        Poll::Pending
                    }
                }
            }
        } else {
            Poll::Pending
        }
    }
}

pub struct ApplyPackageProgression {
    pub operation_idx: usize,
    pub delta_applied_files: usize,
    pub delta_input_bytes: u64,
    pub delta_output_bytes: u64,
}

pub(crate) fn apply_package(
    update_options: UpdateOptions,
    file_manager: WorkspaceFileManager,
    package_name: &metadata::CleanName,
    operations: Vec<(usize, Arc<v1::Operation>)>,
    i_available: AvailableForApply,
) -> ApplyStream {
    let done = Arc::new(AtomicUsize::new(0));
    let o_applied = Arc::new(Mutex::new((VecDeque::new(), AtomicWaker::new())));
    let t_done = done.clone();
    let t_applied = o_applied.clone();
    let t_available = i_available.clone();
    let package_name = package_name.to_string();
    thread::spawn(move || -> () {
        let terr_applied = t_applied.clone();
        let mut applied_data = UpdatePosition::new();
        let base_ctx = HandlerContext {
            file_manager: &file_manager,
            package_name: &package_name,
            operation_idx: 0,
            update_options: &update_options,
        };
        let mut maybe_handler: Option<Box<dyn ApplyHandler>> = None;
        let mut apply_operation = move |operation_idx, operation: &v1::Operation| {
            applied_data.operation_idx = operation_idx;
            applied_data.byte_idx = 0;

            let ctx = HandlerContext { operation_idx, ..base_ctx.clone() };
            let mut handler = match maybe_handler.take() {
                None => operation.apply_handler(ctx)?,
                Some(mut handler) => {
                    if handler.try_still_compatible(operation.path(), operation_idx) {
                        handler
                    } else {
                        operation.apply_handler(ctx)?
                    }
                }
            };

            let data_file_path = handler.download_operation_path();
            let mut maybe_applier = operation.begin_apply(&mut *handler).map_err(|err| {
                warn!(
                    "begin apply operation#{} {} failed: {}",
                    operation_idx,
                    operation.path(),
                    err
                );
                err
            })?;
            debug!("begin apply operation#{} {}", operation_idx, operation.path());
            if let Some(mut applier) = maybe_applier.take() {
                let mut buffer = [0u8; io::BUFFER_SIZE];

                // Wait until there is a least a few bytes available in the current package before
                // opening the file
                t_available.wait_until(|available| applied_data < *available)?;

                let mut total_output_bytes = 0;
                let expected_input_bytes = applier.expected_input_bytes();
                let mut remaining = expected_input_bytes;
                if remaining > 0 {
                    info!("apply data_file_path {:?} for {}", data_file_path, &operation.path());
                    let mut data_file =
                        OpenOptions::new().read(true).open(&data_file_path).map_err(|err| {
                            warn!(
                                "apply operation#{} {} failed: unable to open data file ({})",
                                operation_idx,
                                operation.path(),
                                err
                            );
                            err
                        })?;
                    while remaining > 0 {
                        let available =
                            t_available.wait_until(|available| applied_data < *available)?;
                        let available = if available.operation_idx == applied_data.operation_idx {
                            available.byte_idx - applied_data.byte_idx
                        } else {
                            remaining
                        };

                        let max_read = cmp::min(available, buffer.len() as u64) as usize;
                        let read = data_file
                            .read(&mut buffer[0..max_read])
                            .and_then(|read| {
                                if read > 0 {
                                    Ok(read)
                                } else {
                                    Err(io::Error::new(io::ErrorKind::InvalidData, "EOF"))
                                }
                            })
                            .map_err(|err| {
                                warn!(
                                    "apply operation#{} {} failed: unable to read data file ({})",
                                    operation_idx,
                                    operation.path(),
                                    err
                                );
                                err
                            })?;
                        let new_total_output_bytes =
                            applier.apply_input_bytes(&buffer[0..read]).map_err(|err| {
                                warn!(
                                    "apply operation#{} {} failed: unable to write final file ({})",
                                    operation_idx,
                                    operation.path(),
                                    err
                                );
                                err
                            })?;
                        let delta_input_bytes = read as u64;
                        applied_data.byte_idx += delta_input_bytes;
                        remaining -= delta_input_bytes;

                        let delta_output_bytes = new_total_output_bytes - total_output_bytes;
                        notify(
                            &t_applied,
                            Ok(ApplyPackageProgression {
                                operation_idx: applied_data.operation_idx,
                                delta_applied_files: 0,
                                delta_input_bytes,
                                delta_output_bytes,
                            }),
                        );
                        total_output_bytes = new_total_output_bytes;
                    }
                }

                let mut remaining = applier.expected_check_bytes();
                while remaining > 0 {
                    let delta_bytes = applier.check_bytes(&mut buffer).map_err(|err| {
                        warn!(
                            "apply operation#{} {} failed: unable to check final file ({})",
                            operation_idx,
                            operation.path(),
                            err
                        );
                        err
                    })?;
                    remaining -= delta_bytes;
                    notify(
                        &t_applied,
                        Ok(ApplyPackageProgression {
                            operation_idx: applied_data.operation_idx,
                            delta_applied_files: 0,
                            delta_input_bytes: delta_bytes,
                            delta_output_bytes: delta_bytes,
                        }),
                    );
                }

                applier.commit().map_err(|err| {
                    warn!(
                        "apply operation#{} {} failed: unable to commit changes ({})",
                        operation_idx,
                        operation.path(),
                        err
                    );
                    err
                })?;

                if expected_input_bytes > 0 {
                    io::remove_file(&data_file_path)?;
                }
            }
            drop(maybe_applier);
            applied_data.operation_idx += 1;
            applied_data.byte_idx = 0;
            notify(
                &t_applied,
                Ok(ApplyPackageProgression {
                    operation_idx: applied_data.operation_idx,
                    delta_applied_files: 1,
                    delta_input_bytes: 0,
                    delta_output_bytes: 0,
                }),
            );
            maybe_handler = Some(handler);
            Ok(())
        };

        for &(idx, ref operation) in operations.iter() {
            if let Err(err) = apply_operation(idx, operation) {
                let err = match err {
                    InternalApplyError::IoError(io_err) => ApplyError::OperationFailed {
                        path: operation.path().clone(),
                        slice: operation.slice().cloned(),
                        cause: io_err,
                    },
                    InternalApplyError::Cancelled => ApplyError::Cancelled,
                    InternalApplyError::PoisonError => ApplyError::PoisonError,
                };
                notify(&terr_applied, Err(err));
            }
        }
        t_done.store(1, Ordering::Relaxed);
        notify_end(&terr_applied);
        debug!("end apply");
    });

    ApplyStream { done, o_applied, i_available }
}
