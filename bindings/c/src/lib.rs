use updater::progression::{Progression, TimedProgression};
use updater::repository::{AutoRepository, RemoteRepository, RepositoryFuture};
use updater::storage::Versions;
use updater::update_workspace;
use updater::updater::Error;
use updater::workspace::{State, StateUpdating, Workspace};

use futures::{future, future::IntoFuture, Future};
use log::info;

use std::ffi::CStr;
use std::ffi::CString;
use std::ffi::OsStr;
use std::fs;
use std::io;
use std::os::raw::{c_char, c_void};
use std::path::Path;
use std::ptr;
use std::time::Duration;

#[repr(C)]
pub struct CLocalState {
  pub version: *const c_char,
  pub update_in_progress: u8,
}

#[no_mangle]
pub extern "C" fn c_local_state(
  workspace_path: *const c_char,
  version_callback: extern "C" fn(*const c_char, *const CLocalState, *mut c_void),
  data: *mut c_void,
) -> u8 {
  let _ = env_logger::try_init();
  let workspace_path = unsafe { CStr::from_ptr(workspace_path) }.to_str().unwrap();
  let mut workspace = Workspace::new(Path::new(workspace_path));

  let res = workspace.load_state().map(|_| workspace.state());
  match res {
    Ok(state) => {
      let (version, update_in_progress) = match state {
        State::New => (None, 0),
        State::Stable { version } => (Some(version), 0),
        State::Updating(StateUpdating { from, .. }) => (Some(from), 0),
      };
      let version = version.map(|v| CString::new(v.as_str()).unwrap());
      version_callback(
        ptr::null(),
        &CLocalState {
          version: version.as_ref().map(|v| v.as_ptr()).unwrap_or(ptr::null()),
          update_in_progress,
        },
        data,
      );
      1
    }
    Err(err) => {
      let err = CString::new(format!("{}", err)).unwrap();
      version_callback(err.as_ptr(), ptr::null(), data);
      0
    }
  }
}

#[repr(C)]
pub struct CRemoteVersion {
  pub version: *const c_char,
  pub description: *const c_char,
}

#[no_mangle]
pub extern "C" fn c_version_info(
  repository_url: *const c_char,
  username: *const c_char,
  password: *const c_char,
  version: *const c_char,
  version_callback: extern "C" fn(*const c_char, *const CRemoteVersion, *mut c_void),
  data: *mut c_void,
) -> u8 {
  let _ = env_logger::try_init();
  let repository_url = unsafe { CStr::from_ptr(repository_url) }.to_str().unwrap();
  let version = if version.is_null() {
    None
  } else {
    Some(unsafe { CStr::from_ptr(version) }.to_str().unwrap())
  };
  let auth = if username.is_null() || password.is_null() {
    None
  } else {
    Some((
      unsafe { CStr::from_ptr(username) }.to_str().unwrap(),
      unsafe { CStr::from_ptr(password) }.to_str().unwrap(),
    ))
  };

  let res = AutoRepository::new(repository_url, auth)
    .ok_or(Error::UnsupportedRemote)
    .and_then(|repository| {
      let version: RepositoryFuture<String> = if let Some(version) = version {
        Box::new(future::ok(version.to_owned()))
      } else {
        Box::new(repository.current_version().and_then(|c| {
          info!("latest = {}", c.version());
          Ok(c.version().to_owned())
        }))
      };
      let versions = repository.versions();
      (version, versions)
        .into_future()
        .map(|(version, versions)| match versions {
          Versions::V1 { versions } => versions.into_iter().find(|v| v.revision == version),
        })
        .map_err(Error::RemoteRepository)
        .wait()
    });
  match res {
    Ok(Some(version)) => {
      let revision = CString::new(version.revision.as_str()).unwrap();
      let description = CString::new(version.description.as_str()).unwrap();
      version_callback(
        ptr::null(),
        &CRemoteVersion {
          version: revision.as_ptr(),
          description: description.as_ptr(),
        },
        data,
      );
      1
    }
    Ok(None) => {
      let err = CString::new("version not found").unwrap();
      version_callback(err.as_ptr(), ptr::null(), data);
      0
    }
    Err(err) => {
      let err = CString::new(format!("{}", err)).unwrap();
      version_callback(err.as_ptr(), ptr::null(), data);
      0
    }
  }
}

#[repr(C)]
pub struct CGlobalProgression {
  pub packages_start: usize,
  pub packages_end: usize,

  pub downloaded_files_start: usize,
  pub downloaded_files_end: usize,
  pub downloaded_bytes_start: u64,
  pub downloaded_bytes_end: u64,

  pub applied_files_start: usize,
  pub applied_files_end: usize,
  pub applied_input_bytes_start: u64,
  pub applied_input_bytes_end: u64,
  pub applied_output_bytes_start: u64,
  pub applied_output_bytes_end: u64,

  pub failed_files: usize,

  pub downloaded_files_per_sec: f64,
  pub downloaded_bytes_per_sec: f64,

  pub applied_files_per_sec: f64,
  pub applied_input_bytes_per_sec: f64,
  pub applied_output_bytes_per_sec: f64,
}

#[no_mangle]
pub extern "C" fn c_update_workspace(
  workspace_path: *const c_char,
  repository_url: *const c_char,
  username: *const c_char,
  password: *const c_char,
  goal_version: *const c_char,
  progress_callback: extern "C" fn(*const c_char, *const CGlobalProgression, *mut c_void) -> u8,
  data: *mut c_void,
) -> u8 {
  let _ = env_logger::try_init();
  let workspace_path = unsafe { CStr::from_ptr(workspace_path) }.to_str().unwrap();
  let repository_url = unsafe { CStr::from_ptr(repository_url) }.to_str().unwrap();
  let goal_version = if goal_version.is_null() {
    None
  } else {
    Some(unsafe { CStr::from_ptr(goal_version) }.to_str().unwrap())
  };
  let auth = if username.is_null() || password.is_null() {
    None
  } else {
    Some((
      unsafe { CStr::from_ptr(username) }.to_str().unwrap(),
      unsafe { CStr::from_ptr(password) }.to_str().unwrap(),
    ))
  };
  let mut timed_progression = TimedProgression::new(10, Duration::new(2, 0));
  let res = update_workspace(
    workspace_path,
    repository_url,
    auth,
    goal_version,
    |progress| {
      let speed = timed_progression.add(Progression::from(progress));
      if let Some(speed) = speed {
        let cprogress = CGlobalProgression {
          packages_start: progress.packages.start,
          packages_end: progress.packages.end,
          downloaded_files_start: progress.downloaded_files.start,
          downloaded_files_end: progress.downloaded_files.end,
          downloaded_bytes_start: progress.downloaded_bytes.start,
          downloaded_bytes_end: progress.downloaded_bytes.end,
          applied_files_start: progress.applied_files.start,
          applied_files_end: progress.applied_files.end,
          applied_input_bytes_start: progress.applied_input_bytes.start,
          applied_input_bytes_end: progress.applied_input_bytes.end,
          applied_output_bytes_start: progress.applied_output_bytes.start,
          applied_output_bytes_end: progress.applied_output_bytes.end,
          failed_files: progress.failed_files,
          downloaded_files_per_sec: speed.downloaded_files_per_sec(),
          downloaded_bytes_per_sec: speed.downloaded_bytes_per_sec(),
          applied_files_per_sec: speed.applied_files_per_sec(),
          applied_input_bytes_per_sec: speed.applied_input_bytes_per_sec(),
          applied_output_bytes_per_sec: speed.applied_output_bytes_per_sec(),
        };
        progress_callback(ptr::null(), &cprogress, data) != 0
      } else {
        true
      }
    },
  );
  if let Err(err) = &res {
    let err = CString::new(format!("{}", err)).unwrap();
    progress_callback(err.as_ptr(), ptr::null(), data);
  }
  u8::from(res.is_ok())
}

#[repr(C)]
pub struct CCopyProgression {
  pub files_start: usize,
  pub files_end: usize,
  pub bytes_start: u64,
  pub bytes_end: u64,
  pub failed_files: usize,
}

#[no_mangle]
pub extern "C" fn c_copy_workspace(
  workspace_from: *const c_char,
  workspace_dest: *const c_char,
  progress_callback: extern "C" fn(*const c_char, *const CCopyProgression, *mut c_void) -> u8,
  data: *mut c_void,
) -> u8 {
  let _ = env_logger::try_init();
  let workspace_from = unsafe { CStr::from_ptr(workspace_from) }.to_str().unwrap();
  let workspace_dest = unsafe { CStr::from_ptr(workspace_dest) }.to_str().unwrap();
  let res = copy_dir_recursive(Path::new(workspace_from), Path::new(workspace_dest));

  if let Err(err) = &res {
    let err = CString::new(format!("{}", err)).unwrap();
    progress_callback(err.as_ptr(), ptr::null(), data);
  }
  u8::from(res.is_ok())
}

fn copy_dir_recursive(from: &Path, to: &Path) -> io::Result<()> {
  fs::create_dir_all(to)?;
  for entry in fs::read_dir(from)? {
    let entry = entry?;
    if entry.file_name() != OsStr::new(".update") {
      let from = entry.path();
      let to = to.join(entry.file_name());
      if from.is_dir() {
        copy_dir_recursive(&from, &to)?;
      } else {
        fs::copy(&from, &to)?;
      }
    }
  }
  Ok(())
}
