//! Traits, helpers, and type definitions for core I/O functionality.
pub use std::io::*;
use std::path::Path;
use std::{fmt, fs};

use sha1::{Digest, Sha1};

use crate::metadata::Sha1Hash;

/// Buffer size to use in the whole library
pub const BUFFER_SIZE: usize = 128 * 1024;

/// The ReadSlice trait allows for reading a slice from a source without changing
/// its position.
pub trait ReadSlice {
    /// Read the exact number of bytes required to fill `buf` after seeking to
    /// `pos`. If it succeed, seek back to the original position.
    ///
    /// This function reads as many bytes as necessary to completely fill the
    /// specified buffer `buf`.
    fn read_slice(&mut self, pos: SeekFrom, buf: &mut [u8]) -> Result<()>;
}

/// Default ReadSlice implementation for any implementors of both `Read` and `Seek`
impl<T: Read + Seek> ReadSlice for T {
    fn read_slice(&mut self, pos: SeekFrom, buf: &mut [u8]) -> Result<()> {
        let current = self.seek(SeekFrom::Current(0))?;
        self.seek(pos)?;
        self.read_exact(buf)?;
        self.seek(SeekFrom::Start(current))?;
        Ok(())
    }
}

pub trait Check {
    fn check(&mut self, buf: &[u8]);
}

#[derive(Default)]
pub struct CheckSize {
    pub bytes: u64,
}

impl Check for CheckSize {
    #[inline]
    fn check(&mut self, buf: &[u8]) {
        self.bytes += buf.len() as u64;
    }
}

#[derive(Default)]
pub struct CheckSha1Size {
    pub sha1: Sha1,
    pub bytes: u64,
}

impl CheckSha1Size {
    pub fn sha1(&mut self) -> Sha1Hash {
        Sha1Hash::new(self.sha1.finalize_reset().into())
    }
}

impl Check for CheckSha1Size {
    #[inline]
    fn check(&mut self, buf: &[u8]) {
        self.sha1.update(buf);
        self.bytes += buf.len() as u64;
    }
}

/// Reader adaptor which compute sha1 and count read bytes.
pub struct CheckReader<R, C> {
    pub reader: R,
    pub check: C,
}

impl<R, C: Default> CheckReader<R, C> {
    pub fn new(reader: R) -> Self {
        CheckReader { reader, check: C::default() }
    }
}

impl<R> CheckReader<R, CheckSha1Size> {
    pub fn read_bytes(&self) -> u64 {
        self.check.bytes
    }

    pub fn sha1(&mut self) -> Sha1Hash {
        self.check.sha1()
    }
}

impl<R, C> Read for CheckReader<R, C>
where
    R: Read,
    C: Check,
{
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let n = self.reader.read(buf)?;
        self.check.check(&buf[..n]);
        Ok(n)
    }
}

/// Writer adaptor which compute sha1 and count written bytes.
pub struct CheckWriter<W, C> {
    pub writer: W,
    pub check: C,
}

impl<C: Default> CheckWriter<Sink, C> {
    #[allow(dead_code)]
    pub fn sink() -> Self {
        Self::new(sink())
    }
}

impl<W, C: Default> CheckWriter<W, C> {
    pub fn new(writer: W) -> Self {
        CheckWriter { writer, check: C::default() }
    }
}

impl<T, C> Write for CheckWriter<T, C>
where
    T: Write,
    C: Check,
{
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        let written = self.writer.write(buf)?;
        self.check.check(&buf[..written]);
        Ok(written)
    }

    fn write_all(&mut self, buf: &[u8]) -> Result<()> {
        self.writer.write_all(buf)?;
        self.check.check(buf);
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        self.writer.flush()
    }
}

impl<T, C> ReadSlice for CheckWriter<T, C>
where
    T: ReadSlice,
{
    fn read_slice(&mut self, pos: SeekFrom, buf: &mut [u8]) -> Result<()> {
        self.writer.read_slice(pos, buf)
    }
}

impl<T, C> ReadSlice for &'_ mut CheckWriter<T, C>
where
    T: ReadSlice,
{
    fn read_slice(&mut self, pos: SeekFrom, buf: &mut [u8]) -> Result<()> {
        self.writer.read_slice(pos, buf)
    }
}

pub fn remove_file<P: AsRef<Path>>(path: P) -> Result<()> {
    fs::remove_file(path).or_else(|err| match err.kind() {
        ErrorKind::NotFound => Ok(()),
        _ => Err(err),
    })
}

pub fn atomic_rename<P: AsRef<Path>, Q: AsRef<Path>>(from: P, to: Q) -> Result<()> {
    fs::rename(from, to)
}

pub fn atomic_write_json<P: AsRef<Path>, T>(path: P, value: &T) -> Result<()>
where
    T: serde::Serialize,
{
    let path = path.as_ref();
    let mut tmp_path = path.as_os_str().to_owned();
    tmp_path.push(".tmp");
    {
        let mut file = fs::File::create(&tmp_path)?;
        serde_json::to_writer_pretty(&mut file, value)?;
        file.flush()?;
    }
    let res = atomic_rename(&tmp_path, path);
    if res.is_err() {
        let _ = remove_file(&tmp_path);
    }
    res
}

pub fn assert_eq<T: PartialEq + fmt::Display>(found: T, expected: T, ctx: &str) -> Result<()> {
    if found != expected {
        Err(Error::new(
            ErrorKind::InvalidData,
            format!(
                "{ctx} mismatch, found: {found}, expected: {expected}",
                ctx = ctx,
                found = found,
                expected = expected,
            ),
        ))
    } else {
        Ok(())
    }
}

pub fn assert_is_file_eq<P: AsRef<Path>>(path: P, expected_is_file: bool, ctx: &str) -> Result<()> {
    match fs::metadata(path) {
        Err(err) => match err.kind() {
            ErrorKind::NotFound => assert_eq(false, expected_is_file, ctx),
            _ => Err(err),
        },
        Ok(metadata) => assert_eq(metadata.is_file(), expected_is_file, ctx),
    }
}

pub struct Slice<T> {
    inner: T,
    offset: u64,
    size: u64,
    pos: u64,
}

impl<T: Seek> Slice<T> {
    pub fn new(mut inner: T, offset: u64, size: u64) -> Result<Self> {
        inner.seek(SeekFrom::Start(offset))?;
        Ok(Self { inner, offset, size, pos: 0 })
    }

    fn seek_err(&self, pos: SeekFrom) -> Error {
        Error::new(
            ErrorKind::PermissionDenied,
            format!("cannot seek to {:?}, it's out of slice {}", pos, self.size),
        )
    }
}

impl<T: Seek> Seek for Slice<T> {
    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        let real_pos = match pos {
            SeekFrom::Start(offset) => {
                if offset > self.size {
                    return Err(self.seek_err(pos));
                }
                self.inner.seek(SeekFrom::Start(self.offset + offset))?
            }
            SeekFrom::End(offset) => {
                if offset > 0 || (-offset) as u64 > self.size {
                    return Err(self.seek_err(pos));
                }
                let real_pos = self.offset + self.size - (-offset) as u64;
                self.inner.seek(SeekFrom::Start(real_pos))?
            }
            SeekFrom::Current(offset) => {
                if offset < 0 && (-offset) as u64 > self.pos {
                    return Err(self.seek_err(pos));
                }
                if offset > 0 && offset as u64 > self.size - self.pos {
                    return Err(self.seek_err(pos));
                }
                self.inner.seek(SeekFrom::Current(offset))?
            }
        };
        self.pos = real_pos - self.offset;
        Ok(self.pos)
    }
}

impl<T: Read> Read for Slice<T> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        // Don't call into inner reader at all at EOF because it may still block
        if self.pos == self.size {
            return Ok(0);
        }

        let max = std::cmp::min(buf.len() as u64, self.size - self.pos) as usize;
        let n = self.inner.read(&mut buf[..max])?;
        self.pos += n as u64;
        Ok(n)
    }
}

#[cfg(unix)]
pub fn set_exe_permission(file: &fs::File, exe: bool) -> Result<()> {
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
pub fn set_exe_permission(_file: &fs::File, _exe: bool) -> Result<()> {
    Ok(())
}
