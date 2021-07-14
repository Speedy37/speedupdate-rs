use std::{fs, path::PathBuf};

use super::{Applier, CheckApplier, HandlerContext, WriteApplier};
use crate::codecs::CheckCoder;
use crate::io;
use crate::metadata;

pub struct Handler<'a> {
    ctx: HandlerContext<'a>,
}

impl<'a> Handler<'a> {
    pub fn new(ctx: HandlerContext<'a>) -> Self {
        Self { ctx }
    }
}

impl<'a> super::ApplyHandler for Handler<'a> {
    fn download_operation_path(&self) -> PathBuf {
        self.ctx.download_operation_path()
    }
    fn try_still_compatible(&mut self, _path: &metadata::CleanPath, _operation_idx: usize) -> bool {
        false
    }

    fn add(&mut self, op: &metadata::v1::Add) -> io::Result<Option<Box<dyn Applier>>> {
        let tmp_path = self.ctx.tmp_operation_path();
        let final_path = self.ctx.final_path(&op.common.path);
        let tmp_file = fs::OpenOptions::new().write(true).create(true).open(&tmp_path)?;
        io::set_exe_permission(&tmp_file, op.common.exe)?;
        let decoder = CheckCoder::decoder(&op.data_compression, tmp_file)?;
        let applier = WriteApplier {
            data_size_expected: op.data_size,
            data_sha1_expected: op.data_sha1.clone(),
            final_size_expected: op.final_size,
            final_sha1_expected: op.final_sha1.clone(),
            final_path,
            tmp_path,
            decoder,
        };
        Ok(Some(Box::new(applier)))
    }

    fn patch(&mut self, op: &metadata::v1::Patch) -> io::Result<Option<Box<dyn Applier>>> {
        let final_path = self.ctx.final_path(&op.common.path);
        let current_local_size = fs::metadata(&final_path).map(|m| m.len())?;

        io::assert_eq(current_local_size, op.local_size, "local size")?;

        let local_file = fs::OpenOptions::new().read(true).write(true).open(&final_path)?;
        let tmp_path = self.ctx.tmp_operation_path();
        let tmp_file =
            fs::OpenOptions::new().write(true).read(true).create(true).open(&tmp_path)?;
        io::set_exe_permission(&tmp_file, op.common.exe)?;
        let decoder =
            CheckCoder::patch_decoder(&op.data_compression, &op.patch_type, local_file, tmp_file)?;
        let applier = WriteApplier {
            data_size_expected: op.data_size,
            data_sha1_expected: op.data_sha1.clone(),
            final_size_expected: op.final_size,
            final_sha1_expected: op.final_sha1.clone(),
            final_path,
            tmp_path,
            decoder,
        };
        Ok(Some(Box::new(applier)))
    }

    fn check(&mut self, op: &metadata::v1::Check) -> io::Result<Option<Box<dyn Applier>>> {
        if !self.ctx.update_options.check {
            return Ok(None);
        }

        let path = self.ctx.final_path(&op.common.path);
        let file = fs::OpenOptions::new().read(true).open(&path)?;
        let size = file.metadata()?.len();
        io::assert_eq(size, op.local_size, "local size")?;
        io::set_exe_permission(&file, op.common.exe)?;
        let applier = CheckApplier::new(op.local_size, op.local_sha1.clone(), file);
        Ok(Some(Box::new(applier)))
    }

    fn rm(&mut self, op: &metadata::v1::Rm) -> io::Result<Option<Box<dyn Applier>>> {
        io::remove_file(self.ctx.final_path(&op.path))?;
        Ok(None)
    }

    fn finalize(self: Box<Self>) -> io::Result<Option<Box<dyn Applier>>> {
        Ok(None)
    }

    fn mkdir(&mut self, path: &metadata::CleanPath) -> io::Result<Option<Box<dyn Applier>>> {
        fs::create_dir_all(self.ctx.final_path(path)).map(|_| None).or_else(|err| {
            match err.kind() {
                io::ErrorKind::AlreadyExists => Ok(None),
                _ => Err(err),
            }
        })
    }

    fn rmdir(&mut self, path: &metadata::CleanPath) -> io::Result<Option<Box<dyn Applier>>> {
        if let Err(err) = fs::remove_dir(self.ctx.final_path(path)) {
            if err.kind() != io::ErrorKind::NotFound {
                self.ctx.warn_fs(&format!("unable to remove directory {}", path), err)?;
            }
        }
        Ok(None)
    }
}
