extern crate env_logger;
extern crate updater;

use updater::progression::{Progression, TimedProgression};
use updater::update_workspace;

use std::ffi::CStr;
use std::ffi::CString;
use std::ffi::OsStr;
use std::fs;
use std::io;
use std::os::raw::{c_char, c_int, c_void};
use std::path::Path;
use std::ptr;
use std::time::Duration;

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
pub extern "C" fn c_init_logger(_lvl: c_int) {
  env_logger::init();
}

#[no_mangle]
pub extern "C" fn c_free_string(ptr: *mut c_char) {
  if !ptr.is_null() {
    let _ = unsafe { CString::from_raw(ptr) };
  }
}

#[no_mangle]
pub extern "C" fn c_update_workspace(
  workspace_path: *const c_char,
  repository_url: *const c_char,
  username: *const c_char,
  password: *const c_char,
  goal_version: *const c_char,
  progress_callback: extern "C" fn(*const CGlobalProgression, *mut c_void) -> c_int,
  data: *mut c_void,
) -> *mut c_char {
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
        progress_callback(&cprogress, data) != 0
      } else {
        true
      }
    },
  );
  res
    .map(|_| ptr::null_mut())
    .unwrap_or_else(|err| CString::new(format!("{}", err)).unwrap().into_raw())
}

#[no_mangle]
pub extern "C" fn c_copy_workspace(
  workspace_from: *const c_char,
  workspace_dest: *const c_char,
) -> *mut c_char {
  let workspace_from = unsafe { CStr::from_ptr(workspace_from) }.to_str().unwrap();
  let workspace_dest = unsafe { CStr::from_ptr(workspace_dest) }.to_str().unwrap();
  let res = copy_dir_recursive(Path::new(workspace_from), Path::new(workspace_dest));
  res
    .map(|_| ptr::null_mut())
    .unwrap_or_else(|err| CString::new(format!("{}", err)).unwrap().into_raw())
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
