use std::fs::OpenOptions;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::ops::{Deref, Range};
use std::sync::Arc;
use std::{cmp, pin::Pin};

use futures::prelude::*;
use tracing::{debug, info};

use super::updater::UpdateError;
use crate::link::RemoteRepository;
use crate::metadata::{self, Operation};
use crate::workspace::{UpdatePosition, WorkspaceFileManager};

/// Construct a list of ranges to downloads
fn ranges<'a, L, I>(operations: L, offset: u64, merge_distance: u64) -> Vec<Range<u64>>
where
    L: Iterator<Item = &'a I>,
    I: Operation + 'a,
{
    let mut ranges: Vec<Range<u64>> = Vec::new();
    let mut offset = offset;
    for operation in operations {
        if let Some(range) = operation.range() {
            let start = range.start + offset;
            offset = 0;
            let mut push = true;
            if let Some(last_range) = ranges.last_mut() {
                push = last_range.end + merge_distance < start;
                if !push {
                    last_range.end = range.end;
                }
            }
            if push {
                ranges.push(Range { start: start, end: range.end });
            }
        }
    }
    ranges
}

pub struct DownloadPackageProgression {
    pub(super) available: UpdatePosition,
    pub delta_downloaded_files: usize,
    pub delta_downloaded_bytes: u64,
}

pub type DownloadStream<'a> =
    Pin<Box<dyn Stream<Item = Result<DownloadPackageProgression, UpdateError>> + 'a>>;

/// Download package `package_name` from `repository` and returns a stream of progress
///
/// Downloaded bytes are stored in `file_manager` download_operation_path files
pub(super) fn download_package<'a, R, O>(
    file_manager: WorkspaceFileManager,
    repository: &'a R,
    package_name: &metadata::CleanName,
    operations: Vec<(usize, Arc<O>)>,
    start_position: UpdatePosition,
) -> DownloadStream<'a>
where
    R: RemoteRepository,
    O: Operation + 'a,
{
    // 1. Compute the list of ranges to download in the requested package
    let ranges =
        ranges(operations.iter().map(|&(_, ref o)| o.deref()), start_position.byte_idx, 500 * 1024);
    let mut end_position = start_position.clone();
    if let Some(&(last_op_idx, _)) = operations.last() {
        end_position.operation_idx = last_op_idx + 1;
    }
    debug!("download ranges: {:?}", ranges);

    // 2. Build operations file opener
    let package_name_o = package_name.clone();
    let mut operations_iter = operations.into_iter().filter_map(move |(operation_idx, o)| {
        if let Some(range) = o.range() {
            let data_file_path =
                file_manager.download_operation_path(&package_name_o, operation_idx);
            info!("downl data_file_path {:?} for {}", data_file_path, &o.path());
            let file = OpenOptions::new().write(true).create(true).open(data_file_path);
            Some((operation_idx, range, file, o))
        } else {
            None
        }
    });

    // 2. Starts downloading ranges
    // -> TryStream< (range_start: u64, Bytes) >
    let package_name_r = package_name.clone();
    let download_ranges = stream::iter(ranges.into_iter().map(move |range| {
        let range_start = range.start;
        repository.package(package_name_r.clone(), range).map_err(UpdateError::Download).map_ok(
            move |chunks| {
                chunks.map_ok(move |chunk| (range_start, chunk)).map_err(UpdateError::Download)
            },
        )
    }))
    .then(|fut| fut)
    .try_flatten();

    // 3. Write downloaded ranges chunks
    // -> TryStream< UpdatePosition >
    let mut position = start_position.clone();
    let mut current_operation = None;
    let mut pos = 0;
    let write_ranges = download_ranges.and_then(move |(range_start, chunk)| {
        pos = pos.max(range_start);
        let mut write_downloaded_chunk = || -> Result<DownloadPackageProgression, UpdateError> {
            let mut bytes: &[u8] = &chunk;
            let mut delta_downloaded_files = 0;
            let mut delta_downloaded_bytes = 0;
            loop {
                if current_operation.is_none() {
                    if let Some((operation_idx, range, file, operation)) = operations_iter.next() {
                        debug!(
                            "begin download operation#{} {} [{}, {})",
                            operation_idx,
                            operation.path(),
                            range.start,
                            range.end
                        );
                        let mut file = file.map_err(UpdateError::DownloadCache)?;
                        let pos = if operation_idx == start_position.operation_idx {
                            start_position.byte_idx
                        } else {
                            0
                        };
                        file.set_len(pos).map_err(UpdateError::DownloadCache)?;
                        file.seek(SeekFrom::Start(pos)).map_err(UpdateError::DownloadCache)?;
                        position.operation_idx = operation_idx;
                        position.byte_idx = pos;
                        current_operation = Some((range, file));
                    }
                }
                let done = match (bytes.len(), &mut current_operation) {
                    (0, _) => break,
                    (_, None) => break,
                    (_, Some((range, file))) => {
                        if range.start > pos {
                            // skip unwanted bytes
                            let ignore_len =
                                cmp::min(bytes.len() as u64, range.start - pos) as usize;
                            bytes = &bytes[ignore_len..];
                        }
                        let remaining = (range.end - pos) as usize;
                        let cur_len = cmp::min(bytes.len(), remaining);
                        let cur_bytes = &bytes[0..cur_len];
                        file.write_all(cur_bytes).map_err(UpdateError::DownloadCache)?;
                        bytes = &bytes[cur_len..];
                        {
                            let cur_len = cur_len as u64;
                            position.byte_idx += cur_len;
                            pos += cur_len as u64;
                            delta_downloaded_bytes += cur_len;
                        }
                        remaining == cur_len
                    }
                };

                if done {
                    delta_downloaded_files += 1;
                    position.operation_idx += 1;
                    position.byte_idx = 0;
                    current_operation = None;
                }
            }
            Ok(DownloadPackageProgression {
                available: position.clone(),
                delta_downloaded_files,
                delta_downloaded_bytes,
            })
        };
        future::ready(write_downloaded_chunk())
    });

    let done_stream = future::lazy(move |_| {
        debug!("end download");
        Ok(stream::iter(std::iter::once(Ok(DownloadPackageProgression {
            available: end_position,
            delta_downloaded_files: 0,
            delta_downloaded_bytes: 0,
        }))))
    })
    .try_flatten_stream();

    write_ranges.chain(done_stream).boxed_local()
}
