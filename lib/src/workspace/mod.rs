//! Tools to manage a workspace (update, check, status, ...)
mod apply;
mod check;
mod download;
pub mod progress;
mod updater;

use std::fs;
use std::path::Path;
use std::path::PathBuf;

use futures::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json;

pub use self::check::CheckError;
pub use self::check::GlobalCheckStream;
pub use self::updater::GlobalProgressStream;
pub use self::updater::UpdateError;
pub use self::updater::UpdateOptions;
use crate::io;
use crate::link::RemoteRepository;
use crate::metadata::{self, CleanName};

/// Low level workspace files manager (state.json, check.json, ...)
///
/// Provide access to workspace management files and some utility methods.
#[derive(Clone)]
pub(crate) struct WorkspaceFileManager {
    dir: PathBuf,
}

fn ignore_not_found(res: io::Result<()>) -> io::Result<()> {
    match res {
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
        _ => res,
    }
}

fn clear_dir_contents(dir: &Path) -> io::Result<()> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        let path = &entry.path();
        ignore_not_found(if file_type.is_dir() {
            fs::remove_dir_all(path)
        } else {
            fs::remove_file(path)
        })?;
    }

    Ok(())
}

impl WorkspaceFileManager {
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn create_update_dirs(&self) -> io::Result<()> {
        fs::create_dir_all(self.download_dir())?;
        fs::create_dir_all(self.tmp_dir())?;
        Ok(())
    }

    pub fn clear_tmp_dir(&self) -> io::Result<()> {
        clear_dir_contents(&self.tmp_dir())
    }

    pub fn clear_download_dir(&self) -> io::Result<()> {
        clear_dir_contents(&self.download_dir())
    }

    pub fn metadata_dir(&self) -> PathBuf {
        self.dir().join(".update")
    }

    pub fn state_path(&self) -> PathBuf {
        self.metadata_dir().join("state.json")
    }

    pub fn check_path(&self) -> PathBuf {
        self.metadata_dir().join("check.json")
    }

    pub fn tmp_dir(&self) -> PathBuf {
        self.metadata_dir().join("tmp")
    }

    pub fn download_dir(&self) -> PathBuf {
        self.metadata_dir().join("dl")
    }

    pub fn download_operation_path(&self, package_name: &str, operation_idx: usize) -> PathBuf {
        self.download_dir().join(format!("{}-{}.data", package_name, operation_idx))
    }

    pub fn tmp_operation_path(&self, package_name: &str, operation_idx: usize) -> PathBuf {
        self.tmp_dir().join(format!("{}-{}.tmp", package_name, operation_idx))
    }

    pub fn read_checks(&self) -> io::Result<metadata::WorkspaceChecks> {
        let check_file = fs::File::open(self.check_path())?;
        let checks = serde_json::from_reader(check_file)?;
        Ok(checks)
    }

    pub fn write_checks(&self, checks: &metadata::WorkspaceChecks) -> io::Result<()> {
        io::atomic_write_json(&self.check_path(), &checks)
    }
}

pub struct Workspace {
    file_manager: WorkspaceFileManager,
    state: metadata::WorkspaceState,
}
impl Workspace {
    /// Open workspace
    pub fn open(dir: &Path) -> io::Result<Workspace> {
        let mut workspace = Workspace {
            file_manager: WorkspaceFileManager { dir: dir.to_owned() },
            state: metadata::WorkspaceState::V1 { state: metadata::v1::State::New },
        };
        workspace.reload_state_from_fs()?;
        Ok(workspace)
    }

    /// Cached workspace state
    pub fn state(&self) -> &metadata::v1::State {
        match &self.state {
            metadata::WorkspaceState::V1 { state } => state,
        }
    }

    /// Cached workspace state
    fn state_mut(&mut self) -> &mut metadata::v1::State {
        match &mut self.state {
            metadata::WorkspaceState::V1 { state } => state,
        }
    }

    /// Reload cached workspace state from filesystem
    pub fn reload_state_from_fs(&mut self) -> io::Result<()> {
        let file = fs::File::open(self.file_manager.state_path()).map(|file| Some(file)).or_else(
            |err| match err.kind() {
                io::ErrorKind::NotFound => Ok(None),
                _ => Err(err),
            },
        )?;
        if let Some(file) = file {
            self.state = serde_json::from_reader(file)?;
        }
        Ok(())
    }

    /// Clear update temporary files and reset update progress
    pub fn clear_update_state(&mut self) -> io::Result<()> {
        self.file_manager.clear_download_dir()?;
        self.file_manager.clear_tmp_dir()?;
        match self.state_mut() {
            metadata::v1::State::New
            | metadata::v1::State::Stable { .. }
            | metadata::v1::State::Corrupted { .. } => {}
            metadata::v1::State::Updating(state) => {
                state.clear_progress();
                self.write_state()?;
            }
        }
        Ok(())
    }

    /// Remove all workspace metadata (i.e. '.update' directory and contents)
    pub fn remove_metadata(self) -> io::Result<()> {
        fs::remove_dir_all(self.file_manager.metadata_dir())
    }

    pub(crate) fn set_state(&mut self, state: metadata::v1::State) -> io::Result<()> {
        self.state = metadata::WorkspaceState::V1 { state };
        self.write_state()
    }

    fn write_state(&self) -> io::Result<()> {
        io::atomic_write_json(self.file_manager.state_path(), &self.state)?;
        Ok(())
    }

    pub(crate) fn file_manager(&self) -> WorkspaceFileManager {
        self.file_manager.clone()
    }

    pub fn update<'a, R>(
        &'a mut self,
        repository: &'a R,
        goal_version: Option<CleanName>,
        update_options: UpdateOptions,
    ) -> GlobalProgressStream<'a>
    where
        R: RemoteRepository,
    {
        self::updater::update(self, repository, goal_version, update_options)
            .try_flatten_stream()
            .boxed_local()
    }

    pub fn check<'a>(&'a mut self) -> GlobalCheckStream<'a> {
        self::check::check(self).try_flatten_stream().boxed_local()
    }
}

#[derive(Default, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Clone, Copy, Debug)]
pub(crate) struct UpdatePosition {
    pub operation_idx: usize,
    pub byte_idx: u64,
}

impl UpdatePosition {
    pub fn new() -> UpdatePosition {
        UpdatePosition { operation_idx: 0, byte_idx: 0 }
    }
}
