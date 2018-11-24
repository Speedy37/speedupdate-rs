use futures::{future, stream, Future, Stream};
use operation;
use operation::Operation;
use repository::RemoteRepository;
use std::cmp;
use std::fs::OpenOptions;
use std::io::Seek;
use std::io::Write;
use std::io::SeekFrom;
use std::ops::Range;
use updater::Error;
use workspace::{UpdatePosition, WorkspaceFileManager};

fn ranges<'a, L, I>(operations: L, offset: u64, merge_distance: u64) -> Vec<Range<u64>>
where
  L: Iterator<Item = &'a I>,
  I: operation::Operation + 'a,
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
        ranges.push(Range {
          start: start,
          end: range.end,
        });
      }
    }
  }
  ranges
}

pub fn download_package<'a, R, O>(
  file_manager: WorkspaceFileManager,
  repository: &'a R,
  package_name: String,
  operations: Vec<(usize, O)>,
  start_position: UpdatePosition,
) -> Box<Stream<Item = (UpdatePosition, u64), Error = Error> + 'a>
where
  R: RemoteRepository,
  O: Operation + 'a,
{
  let ranges = ranges(
    operations.iter().map(|&(_, ref o)| o),
    start_position.byte_idx,
    500 * 1024,
  );
  debug!("download ranges: {:?}", ranges);
  let mut operations_iter = operations.into_iter().filter_map(move |(package_idx, o)| {
    if let Some(range) = o.range() {
      let file = OpenOptions::new()
        .write(true)
        .create(true)
        .open(file_manager.download_operation_path(package_idx));
      Some((package_idx, range, file, o))
    } else {
      None
    }
  });

  let mut position = start_position.clone();
  let mut current_operation = None;
  let mut pos = 0;
  let s = stream::iter_ok::<_, Error>(ranges.into_iter().map(move |range| {
    let range_start = range.start;
    repository
      .package(&package_name, range)
      .map_err(Error::RemoteRepository)
      .and_then(move |chunk| Ok((range_start, chunk)))
  })).flatten()
    .and_then(move |(range_start, chunk)| {
      pos = cmp::max(pos, range_start);
      let mut bytes: &[u8] = &chunk;
      let mut total_bytes = 0;
      loop {
        if current_operation.is_none() {
          if let Some((package_idx, range, file, operation)) = operations_iter.next() {
            debug!(
              "begin download operation#{} {} [{}, {})",
              package_idx,
              operation.path(),
              range.start,
              range.end
            );
            let mut file = file?;
            let pos = if package_idx == start_position.package_idx {
              start_position.byte_idx
            } else {
              0
            };
            file.set_len(pos)?;
            file.seek(SeekFrom::Start(pos))?;
            position.package_idx = package_idx;
            position.byte_idx = pos;
            current_operation = Some((range, file));
          }
        }
        let done = match (bytes.len(), &mut current_operation) {
          (0, _) => break,
          (_, &mut None) => break,
          (_, &mut Some((ref range, ref mut file))) => {
            if range.start > pos {
              let ignore_len = cmp::min(bytes.len() as u64, range.start - pos) as usize;
              bytes = &bytes[ignore_len..];
            }
            let remaining = (range.end - pos) as usize;
            let cur_len = cmp::min(bytes.len(), remaining);
            let cur_bytes = &bytes[0..cur_len];
            file.write_all(cur_bytes)?;
            bytes = &bytes[cur_len..];
            {
              let cur_len = cur_len as u64;
              position.byte_idx += cur_len;
              pos += cur_len as u64;
              total_bytes += cur_len;
            }
            remaining == cur_len
          }
        };

        if done {
          position.package_idx += 1;
          position.byte_idx = 0;
          current_operation = None;
        }
      }
      Ok((position.clone(), total_bytes))
    });

  let done_stream = future::lazy(|| {
    debug!("end download");
    Ok(stream::empty())
  }).flatten_stream();

  Box::new(s.chain(done_stream))
}
