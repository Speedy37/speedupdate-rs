//! Sliced file handler
//!
//! This module provide sliced file handling capabilities :
//!
//! - `add`: build a file by appending slices,
//! - `patch`: rebuild an existing file by appending/removing/patching slices,
//! - `check`: check the whole file and each slice,
//!
//! ## Data layout
//!
//! First operation must has no slice and no data.
//! Following operations must have a slice, and must represent a continuous
//! list of slices.
//!
//! ## Recovery
//!
//! This handler allow per slice recovery, ok slices must be present as check
//! operations and first operation is a patch operation.

use std::fmt;
use std::fs::{self, File};
use std::path::PathBuf;

use super::{Applier, CheckApplier, HandlerContext};
use crate::codecs::{self, CheckCoder};
use crate::io::{self, Read, Write};
use crate::metadata::{self, Operation};

pub enum HandlerMode {
    Add { tmp_file: io::CheckWriter<File, io::CheckSha1Size> },
    Patch { local_file: File, tmp_file: io::CheckWriter<File, io::CheckSha1Size> },
    Check { local_file: io::CheckReader<File, io::CheckSha1Size> },
}

impl fmt::Debug for HandlerMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            HandlerMode::Add { .. } => "Add",
            HandlerMode::Patch { .. } => "Patch",
            HandlerMode::Check { .. } => "Check",
        })
    }
}

struct SliceWriteApplier<'a, W> {
    data_size_expected: u64,
    data_sha1_expected: metadata::Sha1Hash,
    final_size_expected: u64,
    final_sha1_expected: metadata::Sha1Hash,
    decoder: codecs::CheckCoder<'a, &'a mut W, io::CheckSha1Size>,
}

impl<'a, W> super::Applier for SliceWriteApplier<'a, W>
where
    &'a mut W: io::Write,
{
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

        Ok(())
    }
}

struct SliceCopyApplier<R, W> {
    size_expected: u64,
    sha1_expected: metadata::Sha1Hash,
    reader: R,
    writer: W,
}

impl<R, W> super::Applier for SliceCopyApplier<io::CheckReader<R, io::CheckSha1Size>, W>
where
    R: io::Read,
    W: io::Write,
{
    fn expected_input_bytes(&self) -> u64 {
        0
    }

    fn apply_input_bytes(&mut self, _buf: &[u8]) -> io::Result<u64> {
        unreachable!()
    }

    fn expected_check_bytes(&mut self) -> u64 {
        self.size_expected
    }

    fn check_bytes(&mut self, buf: &mut [u8]) -> io::Result<u64> {
        let read = self.reader.read(buf)?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "final size mismatch, found: {}, expected: {}",
                    self.reader.check.bytes, self.size_expected
                ),
            ));
        }
        self.writer.write_all(&buf[..read])?;
        Ok(read as u64)
    }

    fn commit(mut self: Box<Self>) -> io::Result<()> {
        let data_sha1 = self.reader.check.sha1();
        io::assert_eq(&data_sha1, &self.sha1_expected, "copy sha1")?;
        let data_size = self.reader.check.bytes;
        io::assert_eq(data_size, self.size_expected, "copy size")?;
        Ok(())
    }
}

pub struct Handler<'a> {
    ctx: HandlerContext<'a>,
    path: metadata::CleanPath,
    final_size_expected: u64,
    final_sha1_expected: metadata::Sha1Hash,
    mode: HandlerMode,
}

impl<'a> Handler<'a> {
    pub fn from_v1_operation(
        ctx: HandlerContext<'a>,
        op: &metadata::v1::Operation,
    ) -> io::Result<Self> {
        let path = op.path();
        let (mode, final_size_expected, final_sha1_expected) = match op {
            metadata::v1::Operation::Add(op) => (
                HandlerMode::Add {
                    tmp_file: io::CheckWriter::new(fs::File::create(ctx.tmp_operation_path())?),
                },
                op.final_size,
                op.final_sha1.clone(),
            ),
            metadata::v1::Operation::Patch(op) => (
                HandlerMode::Patch {
                    tmp_file: io::CheckWriter::new(fs::File::create(ctx.tmp_operation_path())?),
                    local_file: fs::OpenOptions::new()
                        .read(true)
                        .write(true)
                        .open(ctx.final_path(path))?,
                },
                op.final_size,
                op.final_sha1.clone(),
            ),
            metadata::v1::Operation::Check(op) => (
                HandlerMode::Check {
                    local_file: io::CheckReader::new(fs::File::open(ctx.final_path(path))?),
                },
                op.local_size,
                op.local_sha1.clone(),
            ),
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    format!("ue4pak is only support for add, patch and check operations"),
                ))
            }
        };
        Ok(Self { ctx, path: path.clone(), mode, final_size_expected, final_sha1_expected })
    }
}

impl<'a> super::ApplyHandler for Handler<'a> {
    fn download_operation_path(&self) -> PathBuf {
        self.ctx.download_operation_path()
    }

    fn try_still_compatible(&mut self, path: &metadata::CleanPath, operation_idx: usize) -> bool {
        self.ctx.operation_idx = operation_idx;
        &self.path == path
    }

    fn add(&mut self, op: &metadata::v1::Add) -> io::Result<Option<Box<dyn Applier + '_>>> {
        let slice = match &op.common.slice {
            None => return Ok(None),
            Some(slice) => slice,
        };

        match &mut self.mode {
            HandlerMode::Add { tmp_file } | HandlerMode::Patch { tmp_file, .. } => {
                let decoder = CheckCoder::decoder(&op.data_compression, tmp_file)?;
                let applier = SliceWriteApplier {
                    data_size_expected: op.data_size,
                    data_sha1_expected: op.data_sha1.clone(),
                    final_size_expected: op.final_size,
                    final_sha1_expected: op.final_sha1.clone(),
                    decoder,
                };
                Ok(Some(Box::new(applier)))
            }
            HandlerMode::Check { .. } => {
                self.ctx.warn_meta(&format!(
                    "cannot add slice {} to checked pak file {}",
                    slice, op.common.path
                ))?;
                Ok(None)
            }
        }
    }

    fn patch(&mut self, op: &metadata::v1::Patch) -> io::Result<Option<Box<dyn Applier + '_>>> {
        let slice = match &op.common.slice {
            None => return Ok(None),
            Some(slice) => slice,
        };

        match &mut self.mode {
            HandlerMode::Patch { tmp_file, local_file } => {
                let local_slice = io::Slice::new(local_file, op.local_offset, op.local_size)?;
                let decoder = CheckCoder::patch_decoder(
                    &op.data_compression,
                    &op.patch_type,
                    local_slice,
                    tmp_file,
                )?;
                let applier = SliceWriteApplier {
                    data_size_expected: op.data_size,
                    data_sha1_expected: op.data_sha1.clone(),
                    final_size_expected: op.final_size,
                    final_sha1_expected: op.final_sha1.clone(),
                    decoder,
                };
                Ok(Some(Box::new(applier)))
            }
            HandlerMode::Add { .. } => {
                self.ctx.warn_meta(&format!(
                    "cannot patch slice {} to new file {}",
                    slice, op.common.path
                ))?;
                Ok(None)
            }
            HandlerMode::Check { .. } => {
                self.ctx.warn_meta(&format!(
                    "cannot patch slice {} to checked file {}",
                    slice, op.common.path
                ))?;
                Ok(None)
            }
        }
    }

    fn check(&mut self, op: &metadata::v1::Check) -> io::Result<Option<Box<dyn Applier + '_>>> {
        let slice = match &op.common.slice {
            None => return Ok(None), // Check integrity at finalize step
            Some(slice) => slice,
        };

        if matches!(&self.mode, HandlerMode::Check { .. }) && !self.ctx.update_options.check {
            return Ok(None);
        }

        match &mut self.mode {
            HandlerMode::Add { .. } => {
                self.ctx.warn_meta(&format!(
                    "cannot check validity of non existant local file for {}",
                    slice
                ))?;
                Ok(None)
            }
            HandlerMode::Patch { local_file, tmp_file } => {
                let local_slice = io::Slice::new(local_file, op.local_offset, op.local_size)?;
                let applier = SliceCopyApplier {
                    size_expected: op.local_size,
                    sha1_expected: op.local_sha1.clone(),
                    reader: io::CheckReader::new(local_slice),
                    writer: tmp_file,
                };
                Ok(Some(Box::new(applier)))
            }
            HandlerMode::Check { local_file } => {
                io::assert_eq(local_file.check.bytes, op.local_offset, "slice local offset")?;
                let local_slice = local_file.take(op.local_size);
                let applier = CheckApplier::new(op.local_size, op.local_sha1.clone(), local_slice);
                Ok(Some(Box::new(applier)))
            }
        }
    }

    fn rm(&mut self, op: &metadata::v1::Rm) -> io::Result<Option<Box<dyn Applier>>> {
        if op.slice.is_none() {
            self.ctx.warn_meta(&format!(
                "rm {} is not a valid sliced operation without slice",
                op.path
            ))?;
        }
        Ok(None)
    }

    fn mkdir(&mut self, path: &metadata::CleanPath) -> io::Result<Option<Box<dyn Applier>>> {
        self.ctx.warn_meta(&format!("mkdir {} is not a valid sliced operation", path))?;
        Ok(None)
    }

    fn rmdir(&mut self, path: &metadata::CleanPath) -> io::Result<Option<Box<dyn Applier>>> {
        self.ctx.warn_meta(&format!("rmdir {} is not a valid sliced operation", path))?;
        Ok(None)
    }

    fn finalize(self: Box<Self>) -> io::Result<Option<Box<dyn Applier>>> {
        match self.mode {
            HandlerMode::Add { tmp_file } | HandlerMode::Patch { tmp_file, .. } => {
                let mut output_checks = tmp_file.check;
                let final_size = output_checks.bytes;
                io::assert_eq(final_size, self.final_size_expected, "file size")?;
                let final_sha1 = output_checks.sha1();
                io::assert_eq(&final_sha1, &self.final_sha1_expected, "file sha1")?;

                let final_path = self.ctx.final_path(&self.path);
                io::remove_file(&final_path)?;
                fs::rename(&self.ctx.tmp_operation_path(), &final_path)?;

                Ok(None)
            }
            HandlerMode::Check { mut local_file } => {
                let local_size = local_file.check.bytes;
                io::assert_eq(local_size, self.final_size_expected, "file size")?;
                let local_sha1 = local_file.check.sha1();
                io::assert_eq(&local_sha1, &self.final_sha1_expected, "file sha1")?;

                Ok(None)
            }
        }
    }
}
