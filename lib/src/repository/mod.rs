//! Tools to manage a repository
//!
//! ## Repository file system layout :
//!
//! - `current`: a very small JSON file with informations about the current version.
//! - `versions`: a JSON file with informations about all versions.
//! - `packages`: a JSON file that list available packages (i.e. the update graph).
//! - `$package_name.metadata`: a JSON file with precise informations about a package
//!    and how to apply it.
//! - `$package_name`: a binary file containing package update operations data.
//!
//! ## Safety
//!
//! In order to have zero downtime, it's important to only do atomic update
//! (i.e. renaming of existing file) of  repository known files (i.e. `current`,
//! `versions` and `packages`).
mod packager;
pub mod progress;

use std::fs;
use std::path::{Path, PathBuf};

use serde::Serialize;
use serde_json;

pub use self::packager::{BuildError, BuildOptions, PackageBuilder};
pub use crate::codecs::CoderOptions;
use crate::metadata::{self, CleanName, PackageMetadata, Packages, Versions};
use crate::{io, link};

/// Manage a repository (get/set current version, add/rm package, ...)
pub struct Repository {
    dir: PathBuf,
}

impl Repository {
    pub fn new(dir: PathBuf) -> Repository {
        Repository { dir }
    }

    pub fn link(&self) -> link::FileRepository {
        link::FileRepository::new(self.dir.clone())
    }

    pub fn init(&mut self) -> io::Result<()> {
        create_if_missing(
            &self.dir.join(metadata::Versions::filename()),
            &Versions::V1 { versions: Vec::new() },
        )?;
        create_if_missing(
            &self.dir.join(metadata::Packages::filename()),
            &Packages::V1 { packages: Vec::new() },
        )?;
        Ok(())
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn current_version(&self) -> io::Result<metadata::Current> {
        serde_json::from_reader(fs::File::open(self.dir.join(metadata::Current::filename()))?)
            .map_err(io::Error::from)
    }

    /// Set repository current version
    ///
    /// Fails if the request version isn't in the list of known versions or
    /// if the atomic rename of `current` fails
    pub fn set_current_version(&mut self, version: &CleanName) -> io::Result<()> {
        let version: metadata::Current = match self.versions()? {
            Versions::V1 { versions } => versions
                .into_iter()
                .find(|v| &v.revision == version)
                .map(|v| metadata::Current::V1 { current: v }),
        }
        .ok_or(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("version {} doesn't exists", version),
        ))?;
        io::atomic_write_json(&self.dir.join(metadata::Current::filename()), &version)?;
        Ok(())
    }

    pub fn versions(&self) -> io::Result<metadata::Versions> {
        serde_json::from_reader(fs::File::open(self.dir.join(metadata::Versions::filename()))?)
            .map_err(io::Error::from)
    }

    /// Register or update version
    ///
    /// Fails if the atomic rename of `versions` fails.
    pub fn register_version(&self, version: &dyn metadata::Version) -> io::Result<()> {
        let versions = match self.versions()? {
            Versions::V1 { versions } => versions
                .into_iter()
                .filter(|v| &v.revision != version.revision())
                .chain(std::iter::once(metadata::v1::Version {
                    revision: version.revision().clone(),
                    description: version.description().to_owned(),
                }))
                .collect(),
        };
        let versions = Versions::V1 { versions };
        io::atomic_write_json(&self.dir.join(metadata::Versions::filename()), &versions)?;
        Ok(())
    }

    /// Remove version to repository
    ///
    /// Fails if the atomic rename of `versions` fails.
    pub fn unregister_version(&self, revision: &CleanName) -> io::Result<()> {
        let versions = match self.versions()? {
            Versions::V1 { versions } => {
                versions.into_iter().filter(|v| &v.revision != revision).collect()
            }
        };
        let versions = Versions::V1 { versions };
        io::atomic_write_json(&self.dir.join(metadata::Versions::filename()), &versions)?;
        Ok(())
    }

    pub fn packages(&self) -> io::Result<metadata::Packages> {
        serde_json::from_reader(fs::File::open(self.dir.join(metadata::Packages::filename()))?)
            .map_err(io::Error::from)
    }

    pub fn package_metadata(
        &self,
        package_metadata_name: &str,
    ) -> io::Result<metadata::PackageMetadata> {
        serde_json::from_reader(fs::File::open(self.dir.join(package_metadata_name))?)
            .map_err(io::Error::from)
    }

    /// Register or update package to repository
    ///
    /// Fails if the atomic rename of `packages` fails.
    pub fn register_package(&self, package_metadata_name: &str) -> io::Result<()> {
        let packages = match (self.package_metadata(package_metadata_name)?, self.packages()?) {
            (PackageMetadata::V1 { package, .. }, Packages::V1 { packages }) => packages
                .into_iter()
                .filter(|p| p != &package)
                .chain(std::iter::once(package.clone()))
                .collect(),
        };
        let packages = Packages::V1 { packages };
        io::atomic_write_json(&self.dir.join(metadata::Packages::filename()), &packages)?;
        Ok(())
    }

    /// Unregister package to repository
    ///
    /// Fails if the atomic rename of `packages` fails.
    pub fn unregister_package(&self, package_metadata_name: &str) -> io::Result<()> {
        let packages = match (self.package_metadata(package_metadata_name)?, self.packages()?) {
            (PackageMetadata::V1 { package, .. }, Packages::V1 { packages }) => {
                packages.into_iter().filter(|p| p != &package).collect()
            }
        };
        let packages = Packages::V1 { packages };
        io::atomic_write_json(&self.dir.join(metadata::Packages::filename()), &packages)?;
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

#[cfg(test)]
mod tests {
    use futures::TryStreamExt;

    use super::*;
    use crate::workspace::UpdateOptions;
    use crate::Workspace;

    #[test]
    fn create_patch_v1_to_v2() {
        crate::tests::init();
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let v1 = CleanName::from_static_str("1");
        let v2 = CleanName::from_static_str("2");
        let path = crate::tests::tmp_clone_dir("repo_v1_rev1", "create_patch");
        let repository = Repository::new(path.clone());
        let build_directory = path.join("build");
        let previous_dir = build_directory.join(".previous");
        let mut workspace = Workspace::open(&previous_dir).unwrap();
        let link = repository.link();
        let update_stream = workspace.update(&link, Some(v1.clone()), UpdateOptions::default());
        rt.block_on(update_stream.try_for_each(|_| async { Ok(()) })).unwrap();
        workspace.remove_metadata().unwrap();

        let source_directory = crate::tests::data("rev2");
        let mut builder = PackageBuilder::new(build_directory, v2, source_directory);
        builder.set_previous(v1, previous_dir);
        let build_stream = builder.build();
        rt.block_on(build_stream.try_for_each(|_| async { Ok(()) })).unwrap();
    }
}
