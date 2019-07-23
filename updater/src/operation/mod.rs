use crate::updater::UpdateOptions;
use crate::workspace::WorkspaceFileManager;
use crate::BUFFER_SIZE;
use sha1::Sha1;
use std::cell::RefCell;
use std::fs;
use std::io;
use std::io::Read;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::rc::Rc;

pub struct FinalWriterStats {
  pub sha1: Sha1,
  pub written_bytes: u64,
}

pub struct FinalWriter<T> {
  pub inner: T,
  pub stats: Rc<RefCell<FinalWriterStats>>,
}

impl<T> FinalWriter<T> {
  pub fn new(inner: T) -> FinalWriter<T> {
    FinalWriter {
      inner,
      stats: Rc::new(RefCell::new(FinalWriterStats {
        sha1: Sha1::new(),
        written_bytes: 0,
      })),
    }
  }

  pub fn stats(&self) -> Rc<RefCell<FinalWriterStats>> {
    self.stats.clone()
  }
}

impl<T> io::Write for FinalWriter<T>
where
  T: io::Write,
{
  fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
    let written = self.inner.write(buf)?;
    let mut stats = self.stats.borrow_mut();
    stats.sha1.update(&buf[0..written]);
    stats.written_bytes += written as u64;
    Ok(written)
  }

  fn flush(&mut self) -> io::Result<()> {
    self.inner.flush()
  }
}

impl<T> io::Seek for FinalWriter<T>
where
  T: io::Seek,
{
  fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
    self.inner.seek(pos)
  }
}

impl<T> io::Read for FinalWriter<T>
where
  T: io::Read,
{
  fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
    self.inner.read(buf)
  }
}

pub struct ApplyGuard {
  data_size: u64,
  data_sha1: Sha1,
  data_sha1_expected: [u8; 20],
  final_size: u64,
  final_sha1_expected: [u8; 20],
  final_path: PathBuf,
  tmp_stats: Rc<RefCell<FinalWriterStats>>,
  tmp_path: PathBuf,
  decoder: Option<Box<io::Write>>,
}

impl ApplyGuard {
  pub fn new(
    data_size: u64,
    data_sha1_expected: [u8; 20],
    final_size: u64,
    final_sha1_expected: [u8; 20],
    final_path: PathBuf,
    tmp_stats: Rc<RefCell<FinalWriterStats>>,
    tmp_path: PathBuf,
    decoder: Box<io::Write>,
  ) -> ApplyGuard {
    ApplyGuard {
      data_size,
      data_sha1: Sha1::new(),
      data_sha1_expected,
      final_size,
      final_sha1_expected,
      final_path,
      tmp_stats,
      tmp_path,
      decoder: Some(decoder),
    }
  }
  pub fn data_size(&self) -> u64 {
    self.data_size
  }

  pub fn written_bytes(&self) -> u64 {
    self.tmp_stats.borrow().written_bytes
  }

  pub fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
    self.data_sha1.update(buf);
    self.decoder.as_mut().unwrap().write_all(buf)
  }

  pub fn commit(&mut self) -> io::Result<()> {
    self.decoder.as_mut().unwrap().flush()?;
    self.decoder = None;
    if self.data_sha1.digest().bytes() != self.data_sha1_expected {
      return Err(io::Error::new(
        io::ErrorKind::InvalidData,
        "data sha1 mismatch",
      ));
    }
    if self.tmp_stats.borrow().sha1.digest().bytes() != self.final_sha1_expected {
      return Err(io::Error::new(
        io::ErrorKind::InvalidData,
        "final sha1 mismatch",
      ));
    }
    if self.tmp_stats.borrow().written_bytes != self.final_size {
      return Err(io::Error::new(
        io::ErrorKind::InvalidData,
        "final size mismatch",
      ));
    }
    fs::remove_file(&self.final_path).or_else(|err| match err.kind() {
      io::ErrorKind::NotFound => Ok(()),
      _ => Err(err),
    })?;
    fs::rename(&self.tmp_path, &self.final_path)?;
    Ok(())
  }
}

#[cfg(unix)]
pub(crate) fn check_permission(file: &fs::File, exe: bool) -> io::Result<()> {
  use std::os::unix::fs::PermissionsExt;
  if exe {
    let mut perms = file.metadata()?.permissions();
    let mode = perms.mode();
    if (mode & 0o111) != 0o111 {
      perms.set_mode(mode | 0o111);
      file.set_permissions(perms)?;
    }
  }
  Ok(())
}

#[cfg(not(unix))]
pub(crate) fn check_permission(_file: &fs::File, _exe: bool) -> io::Result<()> {
  Ok(())
}

pub fn check_file(
  path: &Path,
  expected_size: u64,
  expected_sha1: [u8; 20],
  exe: bool,
) -> io::Result<()> {
  let size = fs::metadata(&path).map(|m| m.len())?;
  if size != expected_size {
    Err(io::Error::new(
      io::ErrorKind::InvalidInput,
      format!(
        "current_local_size {} != operation.local_size {}",
        size, expected_size
      ),
    ))
  } else {
    let mut file = fs::OpenOptions::new().read(true).open(&path)?;
    let mut sha1 = Sha1::new();
    let mut buffer = [0u8; BUFFER_SIZE];
    let mut read = file.read(&mut buffer)?;
    while read > 0 {
      sha1.update(&buffer[0..read]);
      read = file.read(&mut buffer)?;
    }
    if sha1.digest().bytes() != expected_sha1 {
      Err(io::Error::new(
        io::ErrorKind::InvalidData,
        "local sha1 mismatch",
      ))
    } else {
      check_permission(&file, exe)
    }
  }
}

pub trait Operation {
  fn path(&self) -> &str;
  fn range(&self) -> Option<Range<u64>>;
  fn data_size(&self) -> u64;
  fn final_size(&self) -> u64;
  fn begin_apply(
    &self,
    file_manager: &WorkspaceFileManager,
    index: usize,
    update_options: &UpdateOptions,
  ) -> Result<Option<ApplyGuard>, io::Error>;
}
