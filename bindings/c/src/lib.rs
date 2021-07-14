use std::ffi::CString;
use std::ffi::OsStr;
use std::fs;
use std::io;
use std::os::raw::{c_char, c_void};
use std::path::Path;
use std::ptr;
use std::{ffi::CStr, ops::Deref};

use futures::prelude::*;
use log::info;
use speedupdate::link::{AutoRepository, RemoteRepository};
use speedupdate::metadata::v1::State;
use speedupdate::metadata::{CleanName, Versions};
use speedupdate::workspace::progress::SharedUpdateProgress;
use speedupdate::workspace::{UpdateError, UpdateOptions, Workspace};

#[repr(C)]
pub struct CLocalState {
    pub version: *const c_char,
    pub update_in_progress: u8,
    pub need_repair: u8,
}

#[no_mangle]
pub extern "C" fn c_local_state(
    workspace_path: *const c_char,
    version_callback: extern "C" fn(*const c_char, *const CLocalState, *mut c_void),
    data: *mut c_void,
) -> u8 {
    let _ = env_logger::try_init();
    let workspace_path = unsafe { CStr::from_ptr(workspace_path) }.to_str().unwrap();
    let workspace_res = Workspace::open(Path::new(workspace_path));
    let res = workspace_res.as_ref().map(|workspace| workspace.state());
    match res {
        Ok(state) => {
            let (version, update_in_progress, need_repair) = match state {
                State::New => (None, 0, 0),
                State::Stable { version } => (Some(version), 0, 0),
                State::Corrupted { version, .. } => (Some(version), 0, 1),
                State::Updating(state) => {
                    (state.from.as_ref(), 0, (!state.failures.is_empty()).into())
                }
            };
            let version = version.map(|v| CString::new(v.deref()).unwrap());
            version_callback(
                ptr::null(),
                &CLocalState {
                    version: version.as_ref().map(|v| v.as_ptr()).unwrap_or(ptr::null()),
                    update_in_progress,
                    need_repair,
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

    let res = AutoRepository::new(repository_url, auth).map_err(UpdateError::Repository).and_then(
        |repository| {
            let version = if let Some(version) = version {
                future::ok(CleanName::new(version.to_owned()).unwrap()).boxed_local()
            } else {
                repository
                    .current_version()
                    .map_ok(|c| {
                        info!("latest = {}", c.version());
                        c.version().clone()
                    })
                    .boxed_local()
            };
            let versions = repository.versions();

            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(
                future::try_join(version, versions)
                    .map_ok(|(version, versions)| match versions {
                        Versions::V1 { versions } => {
                            versions.into_iter().find(|v| v.revision == version)
                        }
                    })
                    .map_err(UpdateError::Repository),
            )
        },
    );
    match res {
        Ok(Some(version)) => {
            let revision = CString::new(version.revision.deref()).unwrap();
            let description = CString::new(version.description.as_str()).unwrap();
            version_callback(
                ptr::null(),
                &CRemoteVersion { version: revision.as_ptr(), description: description.as_ptr() },
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
    let res = update_workspace(workspace_path, repository_url, auth, goal_version, |progress| {
        let state = progress.borrow();
        let progress = state.histogram.progress();
        let speed = state.histogram.speed().progress_per_sec();
        let cprogress = CGlobalProgression {
            packages_start: state.downloading_package_idx,
            packages_end: state.steps.len(),
            downloaded_files_start: progress.downloaded_files,
            downloaded_files_end: state.download_files,
            downloaded_bytes_start: progress.downloaded_bytes,
            downloaded_bytes_end: state.download_bytes,
            applied_files_start: progress.applied_files,
            applied_files_end: state.apply_files,
            applied_input_bytes_start: progress.applied_input_bytes,
            applied_input_bytes_end: state.apply_input_bytes,
            applied_output_bytes_start: progress.applied_output_bytes,
            applied_output_bytes_end: state.apply_output_bytes,
            failed_files: progress.failed_files,
            downloaded_files_per_sec: speed.downloaded_files_per_sec,
            downloaded_bytes_per_sec: speed.downloaded_bytes_per_sec,
            applied_files_per_sec: speed.applied_files_per_sec,
            applied_input_bytes_per_sec: speed.applied_input_bytes_per_sec,
            applied_output_bytes_per_sec: speed.applied_output_bytes_per_sec,
        };
        progress_callback(ptr::null(), &cprogress, data) != 0
    });
    if let Err(err) = &res {
        let err = CString::new(format!("{}", err)).unwrap();
        progress_callback(err.as_ptr(), ptr::null(), data);
    }
    u8::from(res.is_ok())
}

fn update_workspace<F>(
    workspace_path: &str,
    repository_url: &str,
    auth: Option<(&str, &str)>,
    goal_version: Option<&str>,
    mut progress_callback: F,
) -> Result<(), UpdateError>
where
    F: FnMut(SharedUpdateProgress) -> bool,
{
    info!(
        "update_workspace {} {} @ {}",
        workspace_path,
        repository_url,
        goal_version.unwrap_or("latest")
    );
    let repository = AutoRepository::new(repository_url, auth).map_err(UpdateError::Repository)?;
    let mut workspace =
        Workspace::open(Path::new(workspace_path)).map_err(UpdateError::LocalWorkspaceError)?;

    let stream = workspace
        .update(
            &repository,
            goal_version.map(|v| CleanName::new(v.to_string()).unwrap()),
            UpdateOptions::default(),
        )
        .try_take_while(|progress| future::ready(Ok(progress_callback(progress.clone()))));
    let work = stream.try_for_each(|_| async { Ok(()) });

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(work)
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
