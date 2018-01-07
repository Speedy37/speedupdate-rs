use std::fs;
use std::ffi::OsString;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use storage::{self, Package, PackageMetadata, Packages, Versions, v1};
use futures::{future, Future, Stream};
use futures_cpupool::{CpuFuture, CpuPool};
use updater::{update, UpdateOptions};
use workspace::Workspace;
use repository::local::LocalRepository;
use tokio_core::reactor::Core;
use sha1::Sha1;
use brotli::CompressorWriter;
use operation::FinalWriter;
use serde_json;
use serde::Serialize;
use BUFFER_SIZE;

pub struct Repository {
  dir: PathBuf,
}

const V1_VERSION: &str = "version";
const V1_VERSIONS: &str = "versions";
const V1_PACKAGES: &str = "packages";

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
    let previous_directory = build_directory.join("previous");
    let pre = match previous_version {
      Some(previous_version) => {
        fs::create_dir_all(&previous_directory)?;
        let mut workspace = Workspace::new(&previous_directory);
        let mut core = Core::new().unwrap();
        let remote = LocalRepository::new(self.dir.clone());
        let update_stream = update(
          &mut workspace,
          &remote,
          previous_version,
          UpdateOptions { check: false },
        );
        let update_future = update_stream.for_each(move |_| Ok(()));
        core
          .run(update_future)
          .map_err(|_| io::Error::new(io::ErrorKind::Other, "unable to restore previous version"))?;
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

fn ordered_dir_list(
  vec: &mut Vec<(OsString, u8)>,
  dir: Option<&Path>,
  offset: u8,
) -> io::Result<()> {
  if let Some(dir) = dir {
    for entry in fs::read_dir(dir)? {
      let entry = entry?;
      let file_name = entry.file_name();
      let file_type = match entry.file_type()? {
        t if t.is_dir() => IS_DIR,
        t if t.is_file() => IS_FILE,
        _ => Err(io::Error::new(
          io::ErrorKind::Other,
          "unsupported file type",
        ))?,
      };
      match vec.binary_search_by_key(&&file_name, |&(ref file_name, _)| file_name) {
        Ok(index) => vec[index].1 &= file_type << offset,
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
  let mut vec = Vec::new();

  ordered_dir_list(&mut vec, src, 0)?;
  ordered_dir_list(&mut vec, pre, 2)?;

  for (file_name, flags) in vec {
    let src_is_dir = (flags & (IS_DIR << 0)) > 0;
    let src_is_file = (flags & (IS_FILE << 0)) > 0;
    let pre_is_dir = (flags & (IS_DIR << 2)) > 0;
    let pre_is_file = (flags & (IS_FILE << 2)) > 0;
    let relative = relative.join(file_name);
    let path = relative.to_str().unwrap();
    if pre_is_dir && !src_is_dir {
      let path = path.to_owned();
      futures.push(pool.spawn_fn(move || Ok((v1::Operation::RmDir { path }, None))));
      // rm dir
    }
    if pre_is_file && !src_is_file {
      // rm file
      let path = path.to_owned();
      futures.push(pool.spawn_fn(move || Ok((v1::Operation::Rm { path }, None))));
    }
    if src_is_dir && !pre_is_dir {
      // mk dir
      let path = path.to_owned();
      futures.push(pool.spawn_fn(move || Ok((v1::Operation::MkDir { path }, None))));
    }
    if src_is_file && !pre_is_file {
      // add file
      let path = path.to_owned();
      let mut src_file = fs::File::open(&src.unwrap().join(&path))?;
      let tmp_path = tmp_dir.join(format!("op_{}.data", futures.len()));
      futures.push(pool.spawn_fn(move || {
        let mut buffer = [0u8; BUFFER_SIZE];
        let mut sha1 = Sha1::new();
        let mut final_size = 0u64;
        let mut read = src_file.read(&mut buffer)?;
        let tmp_file = fs::File::create(&tmp_path)?;
        let tmp_file = FinalWriter::new(tmp_file);
        let stats = tmp_file.stats.clone();
        {
          let mut compressor = CompressorWriter::new(tmp_file, BUFFER_SIZE, 9, 22);
          while read > 0 {
            final_size += read as u64;
            sha1.update(&buffer[0..read]);
            compressor.write(&buffer[0..read])?;
            read = src_file.read(&mut buffer)?;
          }
        }
        let final_sha1 = sha1.digest().to_string();
        let (data_size, data_sha1) = {
          let stats = &*stats.borrow();
          (stats.written_bytes, stats.sha1.digest().to_string())
        };
        Ok((
          v1::Operation::Add {
            path,
            data_compression: String::from("brotli"),
            data_offset: 0,
            data_size,
            data_sha1,
            final_size,
            final_sha1,
          },
          Some(tmp_path),
        ))
      }));
    }
    if src_is_file && pre_is_file {
      // patch || check file
    }

    if src_is_dir || pre_is_dir {
      let src = if src_is_dir {
        Some(src.unwrap().join(&path))
      } else {
        None
      };
      let pre = if pre_is_dir {
        Some(pre.unwrap().join(&path))
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
  }

  Ok(())
}

#[cfg(test)]
mod tests {
  use packager::Repository;
  use std::path::{Path, PathBuf};
  use std::io;
  use std::fs;

  #[test]
  fn package() {
    let mut repository = Repository::new(PathBuf::from("test_repository"));
    fs::create_dir_all("build_repository");
    fs::create_dir_all("test_repository");
    repository.init().expect("init should not fail");
    repository
      .add_package(
        Path::new("build_repository"),
        Path::new("test"),
        "v1",
        "desc v1",
        None,
      )
      .expect("package to succeed");
  }
}
