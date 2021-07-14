mod direct;
mod sliced;

use std::fs;
use std::io::{Read, Write};
use std::path::PathBuf;

pub use direct::Handler as DefaultHandler;
use tracing::warn;

use crate::metadata::{self, Operation};
use crate::workspace::{UpdateOptions, WorkspaceFileManager};
use crate::{codecs, io};

#[derive(Clone)]
pub struct HandlerContext<'a> {
    pub(crate) file_manager: &'a WorkspaceFileManager,
    pub package_name: &'a str,
    pub operation_idx: usize,
    pub update_options: &'a UpdateOptions,
}

impl<'a> HandlerContext<'a> {
    pub fn final_path(&self, path: &metadata::CleanPath) -> PathBuf {
        self.file_manager.dir().join(&path)
    }

    pub fn tmp_operation_path(&self) -> PathBuf {
        self.file_manager.tmp_operation_path(self.package_name, self.operation_idx)
    }

    pub fn download_operation_path(&self) -> PathBuf {
        self.file_manager.download_operation_path(self.package_name, self.operation_idx)
    }

    fn warn_meta(&self, msg: &str) -> io::Result<()> {
        if self.update_options.strict_meta {
            Err(io::Error::new(io::ErrorKind::InvalidInput, msg))
        } else {
            warn!("{}", msg);
            Ok(())
        }
    }

    fn warn_fs(&self, msg_prefix: &str, err: io::Error) -> io::Result<()> {
        if self.update_options.strict_fs {
            Err(err)
        } else {
            warn!("{}{}", msg_prefix, err);
            Ok(())
        }
    }
}

/// Tell how this operation must be handled and applied
pub trait ApplyOperation: Operation {
    fn apply_handler<'a>(&self, ctx: HandlerContext<'a>) -> io::Result<Box<dyn ApplyHandler + 'a>>;
    fn begin_apply<'a>(
        &self,
        handler: &'a mut dyn ApplyHandler,
    ) -> io::Result<Option<Box<dyn Applier + 'a>>>;
}

/// Manage how an operation must be handled (direct, ue4pak, ...)
///
/// If an operation doesn't provide one, it defaults to the `direct` handler.
///
/// Some handlers might not support every operation or slices. For example, the
/// direct handler doesn't support slices and the slice handler doesn't support
/// mkdir and rmdir.
pub trait ApplyHandler {
    fn download_operation_path(&self) -> PathBuf;
    fn try_still_compatible(&mut self, path: &metadata::CleanPath, operation_idx: usize) -> bool;

    fn add(&mut self, op: &metadata::v1::Add) -> io::Result<Option<Box<dyn Applier + '_>>>;
    fn patch(&mut self, op: &metadata::v1::Patch) -> io::Result<Option<Box<dyn Applier + '_>>>;
    fn check(&mut self, op: &metadata::v1::Check) -> io::Result<Option<Box<dyn Applier + '_>>>;
    fn rm(&mut self, op: &metadata::v1::Rm) -> io::Result<Option<Box<dyn Applier + '_>>>;
    fn mkdir(&mut self, path: &metadata::CleanPath) -> io::Result<Option<Box<dyn Applier + '_>>>;
    fn rmdir(&mut self, path: &metadata::CleanPath) -> io::Result<Option<Box<dyn Applier + '_>>>;
    fn finalize(self: Box<Self>) -> io::Result<Option<Box<dyn Applier>>>;
}

/// Apply chunk by chunk the operation work
///
/// The apply process follow the following order:
///
/// ```ignore
/// # fn example(mut applier: Box<dyn Applier>) -> io::Result<()> {
/// # let data = &[];
/// # let buffer = [0u8; io::BUFFER_SIZE];
/// let mut r = applier.expected_input_bytes();
/// while r > 0 {
///    // read data
///    let delta_output_bytes = applier.apply_input_bytes(data)?;
///    r -= data.len();
/// }
/// let mut r = applier.expected_check_bytes();
/// while r > 0 {
///    r -= applier.check_bytes(buffer)?;
/// }
/// applier.commit()
/// # }
/// ```
pub trait Applier {
    /// Amount of input bytes expected to be written through this applier
    /// `write_all` method
    fn expected_input_bytes(&self) -> u64;

    /// Apply input bytes and return the amount written to the output (delta)
    fn apply_input_bytes(&mut self, buf: &[u8]) -> io::Result<u64>;

    /// Amount of bytes expected to be checked through by this applier `check`
    /// method
    fn expected_check_bytes(&mut self) -> u64;

    /// Check some bytes and return the amount checked (delta)
    ///
    /// `buf` is a non empty preallocated buffer with undefined content.
    fn check_bytes(&mut self, buf: &mut [u8]) -> io::Result<u64>;

    /// Do the final checks and commit changes to the workspace
    ///
    /// Most of the times this checks that written bytes matches the requested
    /// hash and rename the temporary file to the final one
    fn commit(self: Box<Self>) -> io::Result<()>;
}

/// Simple write Applier
struct WriteApplier<'a, W> {
    data_size_expected: u64,
    data_sha1_expected: metadata::Sha1Hash,
    final_size_expected: u64,
    final_sha1_expected: metadata::Sha1Hash,
    final_path: PathBuf,
    tmp_path: PathBuf,
    decoder: codecs::CheckCoder<'a, W, io::CheckSha1Size>,
}

impl<W: io::Write + io::Seek + io::Read> Applier for WriteApplier<'_, W> {
    fn expected_input_bytes(&self) -> u64 {
        self.data_size_expected
    }

    fn apply_input_bytes(&mut self, buf: &[u8]) -> io::Result<u64> {
        self.decoder.write_all(buf)?;
        let output_bytes = self.decoder.output_checks().bytes;
        Ok(output_bytes)
    }

    fn expected_check_bytes(&mut self) -> u64 {
        0
    }

    fn check_bytes(&mut self, _buf: &mut [u8]) -> io::Result<u64> {
        unreachable!()
    }

    fn commit(mut self: Box<Self>) -> io::Result<()> {
        self.decoder.flush()?;

        let input_checks = self.decoder.input_checks();
        let data_sha1 = input_checks.sha1();
        io::assert_eq(&data_sha1, &self.data_sha1_expected, "data sha1")?;
        let data_size = input_checks.bytes;
        io::assert_eq(data_size, self.data_size_expected, "data size")?;

        let mut output_checks = self.decoder.finish()?.check;
        let final_sha1 = output_checks.sha1();
        io::assert_eq(&final_sha1, &self.final_sha1_expected, "final sha1")?;
        let final_size = output_checks.bytes;
        io::assert_eq(final_size, self.final_size_expected, "final size")?;

        io::remove_file(&self.final_path)?;
        fs::rename(&self.tmp_path, &self.final_path)?;
        Ok(())
    }
}

/// Simple write Applier
pub struct CheckApplier<R> {
    final_size_expected: u64,
    final_sha1_expected: metadata::Sha1Hash,
    r: io::CheckReader<R, io::CheckSha1Size>,
}

impl<R> CheckApplier<R> {
    pub fn new(final_size: u64, final_sha1: metadata::Sha1Hash, r: R) -> Self {
        Self {
            final_size_expected: final_size,
            final_sha1_expected: final_sha1,
            r: io::CheckReader::new(r),
        }
    }
}

impl<R: io::Read> Applier for CheckApplier<R> {
    fn expected_input_bytes(&self) -> u64 {
        0
    }

    fn apply_input_bytes(&mut self, _buf: &[u8]) -> io::Result<u64> {
        unreachable!()
    }

    fn expected_check_bytes(&mut self) -> u64 {
        self.final_size_expected
    }

    fn check_bytes(&mut self, buf: &mut [u8]) -> io::Result<u64> {
        let read = self.r.read(buf)?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "final size mismatch, found: {}, expected: {}",
                    self.r.read_bytes(),
                    self.final_size_expected
                ),
            ));
        }

        Ok(read as u64)
    }

    fn commit(mut self: Box<Self>) -> io::Result<()> {
        io::assert_eq(self.r.read_bytes(), self.final_size_expected, "final size")?;
        io::assert_eq(&self.r.sha1(), &self.final_sha1_expected, "final sha1")?;

        Ok(())
    }
}

impl ApplyOperation for metadata::v1::Operation {
    fn apply_handler<'a>(&self, ctx: HandlerContext<'a>) -> io::Result<Box<dyn ApplyHandler + 'a>> {
        if let Some(handler_name) = self.slice_handler() {
            if handler_name.as_str() == "sliced" {
                let handler = sliced::Handler::from_v1_operation(ctx, self)?;
                return Ok(Box::new(handler));
            }

            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("slice handler {} isn't supported!", handler_name),
            ));
        }

        return Ok(Box::new(DefaultHandler::new(ctx)));
    }

    fn begin_apply<'a>(
        &self,
        handler: &'a mut dyn ApplyHandler,
    ) -> io::Result<Option<Box<dyn Applier + 'a>>> {
        match self {
            metadata::v1::Operation::Add(op) => handler.add(op),
            metadata::v1::Operation::Patch(op) => handler.patch(op),
            metadata::v1::Operation::Check(op) => handler.check(op),
            metadata::v1::Operation::MkDir { path, .. } => handler.mkdir(path),
            metadata::v1::Operation::RmDir { path, .. } => handler.rmdir(path),
            metadata::v1::Operation::Rm(op) => handler.rm(op),
        }
    }
}
