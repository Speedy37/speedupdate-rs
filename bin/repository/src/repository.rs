use futures::{future, Future, Stream};
use futures_cpupool::{CpuFuture, CpuPool};
use serde::Serialize;
use serde_json;
use sha1::Sha1;
use std::ffi::{OsStr, OsString};
use std::fs;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::process;
use tokio_core::reactor::Core;

use updater::repository::local::LocalRepository;
use updater::storage::{self, v1, Package, PackageMetadata, Packages, Versions};
use updater::updater::{update, UpdateOptions};
use updater::workspace::Workspace;
use updater::BUFFER_SIZE;

const V1_VERSION: &str = "version";
const V1_VERSIONS: &str = "versions";
const V1_PACKAGES: &str = "packages";

pub struct Repository {
  dir: PathBuf,
}

fn compute_size_and_sha1(path: &Path) -> io::Result<(u64, String)> {
  let size = fs::metadata(&path)?.len();
  let sha1 = {
    let mut buffer = [0u8; BUFFER_SIZE];
    let mut sha1 = Sha1::new();
    let mut file = fs::File::open(&path)?;
    let mut read = file.read(&mut buffer)?;
    while read > 0 {
      sha1.update(&buffer[0..read]);
      read = file.read(&mut buffer)?;
    }
    sha1.digest().to_string()
  };
  Ok((size, sha1))
}

impl Repository {
  pub fn new(dir: PathBuf) -> Repository {
    Repository { dir }
  }

  pub fn init(&mut self) -> io::Result<()> {
    create_if_missing(
      &self.dir.join(V1_VERSIONS),
      &Versions::V1 {
        versions: Vec::new(),
      },
    )?;
    create_if_missing(
      &self.dir.join(V1_PACKAGES),
      &Packages::V1 {
        packages: Vec::new(),
      },
    )?;
    Ok(())
  }

  pub fn dir(&self) -> &Path {
    &self.dir
  }

  pub fn current_version(&self) -> io::Result<storage::Current> {
    serde_json::from_reader(fs::File::open(self.dir.join(V1_VERSION))?).map_err(io::Error::from)
  }

  pub fn set_current_version(&mut self, current_version: storage::Current) -> io::Result<()> {
    write_json(&self.dir, V1_VERSION, &current_version).map(|_| ())
  }

  pub fn versions(&self) -> io::Result<storage::Versions> {
    serde_json::from_reader(fs::File::open(self.dir.join(V1_VERSIONS))?).map_err(io::Error::from)
  }

  pub fn packages(&self) -> io::Result<storage::Packages> {
    serde_json::from_reader(fs::File::open(self.dir.join(V1_PACKAGES))?).map_err(io::Error::from)
  }

  pub fn package_metadata(&self, package_name: &str) -> io::Result<storage::PackageMetadata> {
    serde_json::from_reader(fs::File::open(self.dir.join(package_name))?).map_err(io::Error::from)
  }

  pub fn add_package(
    &mut self,
    build_directory: &Path,
    source_directory: &Path,
    version: &str,
    description: &str,
    previous_version: Option<&str>,
  ) -> io::Result<()> {
    info!(
      "add_package from {} to {{ path = {:?}, version = {} }}",
      previous_version.unwrap_or("nothing"),
      source_directory,
      version
    );
    let previous_directory = build_directory.join("previous");
    let pre = match previous_version {
      Some(previous_version) => {
        fs::create_dir_all(&previous_directory)?;
        let mut workspace = Workspace::new(&previous_directory);
        let mut core = Core::new().unwrap();
        let remote = LocalRepository::new(self.dir.clone());
        {
          let update_stream = update(
            &mut workspace,
            &remote,
            previous_version,
            UpdateOptions { check: false },
          );
          let update_future = update_stream.for_each(move |_| Ok(()));
          core.run(update_future).map_err(|err| {
            println!("err= {:?}", err);
            io::Error::new(io::ErrorKind::Other, "unable to restore previous version")
          })?;
        }
        fs::remove_dir_all(workspace.file_manager().update_dir())?;
        Some(Path::new(&previous_directory))
      }
      _ => None,
    };

    let mut futures = Vec::new();
    let cpu_pool = CpuPool::new(1);
    build_operations(
      &cpu_pool,
      &mut futures,
      build_directory,
      Some(source_directory),
      pre,
      Path::new(""),
    )?;
    let mut operations = future::join_all(futures).wait()?;
    let mut offset: u64 = 0;
    let data_path = build_directory.join("op_all.data");
    let mut data_file = fs::File::create(&data_path)?;
    for operation in operations.iter_mut() {
      match operation.0 {
        v1::Operation::Add {
          ref mut data_offset,
          ..
        }
        | v1::Operation::Patch {
          ref mut data_offset,
          ..
        } => *data_offset = offset,
        _ => {}
      };
      if let Some(ref tmp_path) = operation.1 {
        let mut tmp_file = fs::File::open(&tmp_path)?;
        offset += io::copy(&mut tmp_file, &mut data_file)?;
        fs::remove_file(tmp_path)?;
      }
    }
    let operations: Vec<_> = operations.into_iter().map(|(o, _)| o).collect();
    let version_v1 = v1::Version {
      revision: version.to_owned(),
      description: description.to_owned(),
    };
    let package_v1 = v1::Package {
      from: previous_version.unwrap_or("").to_owned(),
      to: version.to_owned(),
      size: offset,
    };
    let package_metadata_v1 = PackageMetadata::V1 {
      package: package_v1.clone(),
      operations,
    };

    let metadata_path = write_json(build_directory, "op_all.metadata", &package_metadata_v1)?;

    let packages_path = {
      let mut packages = self.packages()?;
      match packages {
        Packages::V1 { ref mut packages } => packages.push(package_v1.clone()),
      };
      write_json(build_directory, V1_PACKAGES, &packages)?
    };

    let versions_path = {
      let mut versions = self.versions()?;
      match versions {
        Versions::V1 { ref mut versions } => versions.push(version_v1.clone()),
      };
      write_json(build_directory, V1_VERSIONS, &versions)?
    };

    fs::rename(
      data_path,
      self.dir.join(package_metadata_v1.package_data_name()),
    )?;
    fs::rename(
      metadata_path,
      self.dir.join(package_metadata_v1.package_metadata_name()),
    )?;
    fs::rename(packages_path, self.dir.join(V1_PACKAGES))?;
    fs::rename(versions_path, self.dir.join(V1_VERSIONS))?;

    Ok(())
  }
}

fn create_if_missing<T>(path: &Path, value: &T) -> io::Result<()>
where
  T: Serialize,
{
  if fs::metadata(path).is_err() {
    let file = fs::File::create(&path)?;
    serde_json::to_writer_pretty(file, value)?;
  }
  Ok(())
}

fn write_json<T>(build_directory: &Path, file_name: &str, value: &T) -> io::Result<PathBuf>
where
  T: Serialize,
{
  let path = build_directory.join(file_name);
  let file = fs::File::create(&path)?;
  serde_json::to_writer_pretty(file, value)?;
  Ok(path)
}

const IS_DIR: u8 = 1;
const IS_FILE: u8 = 2;
const IS_EXE: u8 = 4;

#[cfg(unix)]
fn is_exe(_file_name: &str, metadata: &fs::Metadata) -> bool {
  use std::os::unix::fs::PermissionsExt;
  return metadata.permissions() & 0o444 > 0;
}

#[cfg(not(unix))]
fn is_exe(file_name: &str, _metadata: &fs::Metadata) -> bool {
  return file_name.starts_with(".exe");
}

fn ordered_dir_list(
  vec: &mut Vec<(OsString, u8)>,
  dir: Option<&Path>,
  offset: u8,
) -> io::Result<()> {
  if let Some(dir) = dir {
    for entry in fs::read_dir(dir)? {
      let entry = entry?;
      let file_name = entry.file_name();
      let metadata = entry.metadata()?;
      let mut file_type = match metadata.file_type() {
        t if t.is_dir() => IS_DIR,
        t if t.is_file() => IS_FILE,
        _ => continue,
      };
      if is_exe(&file_name.to_string_lossy(), &metadata) {
        file_type |= IS_EXE;
      }
      match vec.binary_search_by_key(&&file_name, |&(ref file_name, _)| file_name) {
        Ok(index) => vec[index].1 |= file_type << offset,
        Err(index) => vec.insert(index, (file_name, file_type << offset)),
      };
    }
  }
  Ok(())
}

fn build_operations(
  pool: &CpuPool,
  futures: &mut Vec<CpuFuture<(v1::Operation, Option<PathBuf>), io::Error>>,
  tmp_dir: &Path,
  src: Option<&Path>,
  pre: Option<&Path>,
  relative: &Path,
) -> io::Result<()> {
  let brotli_exe = if cfg!(windows) {
    "brotli.exe"
  } else {
    "brotli"
  };
  let vcdiff_exe = if cfg!(windows) {
    "xdelta3.exe"
  } else {
    "xdelta3"
  };
  let mut vec = Vec::new();

  ordered_dir_list(&mut vec, src, 0)?;
  ordered_dir_list(&mut vec, pre, 4)?;

  for (file_name, flags) in vec {
    let src_is_dir = (flags & (IS_DIR << 0)) > 0;
    let src_is_file = (flags & (IS_FILE << 0)) > 0;
    let src_is_exe = (flags & (IS_EXE << 0)) > 0;
    let pre_is_dir = (flags & (IS_DIR << 4)) > 0;
    let pre_is_file = (flags & (IS_FILE << 4)) > 0;
    let pre_is_exe = (flags & (IS_EXE << 4)) > 0;
    let relative = relative.join(&file_name);
    let path = relative.to_str().unwrap();
    if pre_is_file && !src_is_file {
      // rm file
      debug!("rm file {}", path);
      let path = path.to_owned();
      futures.push(pool.spawn_fn(move || Ok((v1::Operation::Rm { path }, None))));
    }
    if src_is_dir && !pre_is_dir {
      // mk dir
      debug!("mk dir {}", path);
      let path = path.to_owned();
      futures.push(pool.spawn_fn(move || Ok((v1::Operation::MkDir { path }, None))));
    }
    if src_is_file && !pre_is_file {
      // add file
      let path = path.to_owned();
      let src_path = src.unwrap().join(&file_name);
      let tmp_path = tmp_dir.join(format!("op_{}.data", futures.len()));
      futures.push(pool.spawn_fn(move || {
        debug!("computing final sha1 {}", path);
        let (final_size, final_sha1) = compute_size_and_sha1(&src_path)?;
        let src_file = fs::File::open(&src_path)?;
        let tmp_file = fs::File::create(&tmp_path)?;
        let mut brotli = process::Command::new(brotli_exe)
          .arg("-9") // write on standard output
          .arg("--stdout") // write on standard output
          .arg("-") // read standard input
          .stdin(process::Stdio::from(src_file))
          .stdout(process::Stdio::from(tmp_file))
          .stderr(process::Stdio::inherit())
          .spawn()?;
        if !brotli.wait()?.success() {
          Err(io::Error::new(
            io::ErrorKind::Other,
            "failed to encode date status code",
          ))?;
        }
        debug!("done brotli data {}", path);
        debug!("computing data sha1 {}", path);
        let (data_size, data_sha1) = compute_size_and_sha1(&tmp_path)?;
        debug!("added {} {} -- brotli --> {}", path, final_size, data_size);
        Ok((
          v1::Operation::Add {
            path,
            data_compression: String::from("brotli"),
            data_offset: 0,
            data_size,
            data_sha1,
            final_size,
            final_sha1,
            exe: src_is_exe,
          },
          Some(tmp_path),
        ))
      }));
    }
    if src_is_file && pre_is_file {
      // patch || check file
      let path = path.to_owned();
      let src_path = src.unwrap().join(&file_name);
      let pre_path = pre.unwrap().join(&file_name);
      let tmp_path = tmp_dir.join(format!("op_{}.data", futures.len()));
      futures.push(pool.spawn_fn(move || {
        debug!("computing previous sha1 {}", path);
        let (local_size, local_sha1) = compute_size_and_sha1(&pre_path)?;
        debug!("computing final sha1 {}", path);
        let (final_size, final_sha1) = compute_size_and_sha1(&src_path)?;
        if final_size == local_size && final_sha1 == local_sha1 {
          debug!("check {}", path);
          Ok((
            v1::Operation::Check {
              path,
              local_size,
              local_sha1,
              exe: src_is_exe,
            },
            None,
          ))
        } else {
          debug!("computing delta {}", path);
          let tmp_file = fs::File::create(&tmp_path)?;
          let mut vcdiff = process::Command::new(vcdiff_exe)
            .arg("-e") // compress
            .arg("-c") // use stdout
            .arg("-s")
            .arg(&pre_path)
            .arg(&src_path)
            .stdout(process::Stdio::piped())
            .stderr(process::Stdio::inherit())
            .spawn()?;
          let mut brotli = process::Command::new(brotli_exe)
            .arg("-9") // write on standard output
            .arg("--stdout") // write on standard output
            .arg("-") // read standard input
            .stdin(process::Stdio::from(vcdiff.stdout.take().unwrap()))
            .stdout(process::Stdio::from(tmp_file))
            .stderr(process::Stdio::inherit())
            .spawn()?;
          if !vcdiff.wait()?.success() {
            debug!("vcdiff failed {:?} {:?} {:?}", src_path, pre_path, tmp_path);
            Err(io::Error::new(
              io::ErrorKind::Other,
              "failed to vcdiff date status code",
            ))?;
          }
          debug!("done vcdiff data {}", path);
          if !brotli.wait()?.success() {
            Err(io::Error::new(
              io::ErrorKind::Other,
              "failed to encode date status code",
            ))?;
          }
          debug!("done brotli data {}", path);
          debug!("computing data sha1 {}", path);
          let (data_size, data_sha1) = compute_size_and_sha1(&tmp_path)?;
          Ok((
            v1::Operation::Patch {
              path,
              data_compression: String::from("brotli"),
              patch_type: String::from("vcdiff"),
              data_offset: 0,
              data_size,
              data_sha1,
              local_size,
              local_sha1,
              final_size,
              final_sha1,
              exe: src_is_exe,
            },
            Some(tmp_path),
          ))
        }
      }));
    }

    if src_is_dir || pre_is_dir {
      let src = if src_is_dir {
        Some(src.unwrap().join(&file_name))
      } else {
        None
      };
      let pre = if pre_is_dir {
        Some(pre.unwrap().join(&file_name))
      } else {
        None
      };
      build_operations(
        pool,
        futures,
        tmp_dir,
        match src {
          Some(ref path) => Some(Path::new(path)),
          None => None,
        },
        match pre {
          Some(ref path) => Some(Path::new(path)),
          None => None,
        },
        &relative,
      )?;
    }

    if pre_is_dir && !src_is_dir {
      debug!("rm dir {}", path);
      let path = path.to_owned();
      futures.push(pool.spawn_fn(move || Ok((v1::Operation::RmDir { path }, None))));
      // rm dir
    }
  }

  Ok(())
}
