use crate::storage::v1;
use serde_json;
use std::fs;
use std::io;
use std::path::Path;
use std::path::PathBuf;

#[derive(Clone)]
pub struct WorkspaceFileManager {
  dir: PathBuf,
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

  pub fn remove_tmp_dir(&self) -> io::Result<()> {
    fs::remove_dir_all(&self.tmp_dir()).or_else(|err| match err.kind() {
      io::ErrorKind::NotFound => Ok(()),
      _ => Err(err),
    })
  }

  pub fn update_dir(&self) -> PathBuf {
    self.dir().join(".update")
  }

  pub fn state_path(&self) -> PathBuf {
    self.update_dir().join("state.json")
  }

  pub fn check_path(&self) -> PathBuf {
    self.update_dir().join("check.json")
  }

  pub fn tmp_dir(&self) -> PathBuf {
    self.update_dir().join("tmp")
  }

  pub fn download_dir(&self) -> PathBuf {
    self.update_dir().join("dl")
  }

  pub fn download_operation_path(&self, index: usize) -> PathBuf {
    self.download_dir().join(format!("operation{}.data", index))
  }

  pub fn tmp_operation_path(&self, index: usize) -> PathBuf {
    self.tmp_dir().join(format!("operation{}.tmp", index))
  }
}

pub struct Workspace {
  dir: PathBuf,
  state: State,
}
impl Workspace {
  pub fn new(dir: &Path) -> Workspace {
    Workspace {
      dir: dir.to_owned(),
      state: State::New,
    }
  }

  pub fn open(dir: &Path) -> io::Result<Workspace> {
    let mut workspace = Workspace::new(dir);
    workspace.load_state()?;
    Ok(workspace)
  }

  pub fn state(&self) -> &State {
    &self.state
  }

  pub fn load_state(&mut self) -> io::Result<()> {
    let file = fs::OpenOptions::new()
      .read(true)
      .create(false)
      .open(self.file_manager().state_path())
      .map(|file| Some(file))
      .or_else(|err| match err.kind() {
        io::ErrorKind::NotFound => Ok(None),
        _ => Err(err),
      })?;
    if let Some(file) = file {
      match serde_json::from_reader(file)? {
        WorkspaceData::V1 { state } => self.state = state,
      }
    }
    Ok(())
  }

  pub fn set_state(&mut self, state: State) -> io::Result<()> {
    self.state = state;
    let file = fs::OpenOptions::new()
      .write(true)
      .create(true)
      .truncate(true)
      .open(self.file_manager().state_path())?;
    let v1 = &WorkspaceData::V1 {
      state: self.state.clone(),
    };
    serde_json::to_writer_pretty(file, v1)?;
    Ok(())
  }

  pub fn file_manager(&self) -> WorkspaceFileManager {
    WorkspaceFileManager {
      dir: self.dir.clone(),
    }
  }
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "version")]
pub enum WorkspaceData {
  #[serde(rename = "1")]
  V1 { state: State },
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "version")]
pub enum CheckPackageMetadata {
  #[serde(rename = "1")]
  V1 { operations: Vec<v1::Operation> },
}

#[derive(Serialize, Deserialize, Clone)]
pub enum State {
  New,
  Stable { version: String },
  Updating(StateUpdating),
}

#[derive(Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Clone, Debug)]
pub struct UpdatePosition {
  pub package_idx: usize,
  pub byte_idx: u64,
}

impl UpdatePosition {
  pub fn new() -> UpdatePosition {
    UpdatePosition {
      package_idx: 0,
      byte_idx: 0,
    }
  }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct StateUpdating {
  pub from: String,
  pub to: String,
  pub available: UpdatePosition,
  pub applied: UpdatePosition,
  pub failures: Vec<String>,
}

impl StateUpdating {
  pub fn new(from: String, to: String) -> StateUpdating {
    StateUpdating {
      from,
      to,
      available: UpdatePosition {
        package_idx: 0,
        byte_idx: 0,
      },
      applied: UpdatePosition {
        package_idx: 0,
        byte_idx: 0,
      },
      failures: Vec::new(),
    }
  }
}
