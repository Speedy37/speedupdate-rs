//! Link to remote repository
mod file;
mod https;

use std::{
    fmt,
    ops::Range,
    path::{Path, PathBuf},
    pin::Pin,
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;

pub use self::file::FileRepository;
pub use self::https::HttpsRepository;
use crate::metadata;

#[derive(Debug)]
pub enum RepositoryError {
    File { path: PathBuf, err: std::io::Error },
    Https(reqwest::Error),
    HttpsNotPartialContent(reqwest::StatusCode),
    Json { path: PathBuf, err: serde_json::Error },
    InvalidUrl { reason: String },
}

impl RepositoryError {
    pub fn file(path: &Path, err: std::io::Error) -> Self {
        RepositoryError::File { path: path.to_owned(), err }
    }

    pub fn json(path: &Path, err: serde_json::Error) -> Self {
        RepositoryError::Json { path: path.to_owned(), err }
    }
}

impl From<reqwest::Error> for RepositoryError {
    fn from(err: reqwest::Error) -> Self {
        RepositoryError::Https(err)
    }
}

impl fmt::Display for RepositoryError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            RepositoryError::File { path, err } => write!(f, "file {:?} error: {}", path, err),
            RepositoryError::Https(err) => err.fmt(f),
            RepositoryError::HttpsNotPartialContent(status) => {
                write!(f, "HTTP status server not partial content ({})", status)
            }
            RepositoryError::Json { path, err } => {
                write!(f, "metadata  {:?} error: {}", path, err)
            }
            RepositoryError::InvalidUrl { reason } => {
                write!(f, "invalid repository url: {}", reason)
            }
        }
    }
}

impl std::error::Error for RepositoryError {}

pub type RepositoryStream<Item> = Pin<Box<dyn Stream<Item = Result<Item, RepositoryError>>>>;

#[async_trait]
pub trait RemoteRepository {
    async fn current_version(&self) -> Result<metadata::Current, RepositoryError>;
    async fn versions(&self) -> Result<metadata::Versions, RepositoryError>;
    async fn packages(&self) -> Result<metadata::Packages, RepositoryError>;
    async fn package_metadata(
        &self,
        package_name: metadata::CleanName,
    ) -> Result<metadata::PackageMetadata, RepositoryError>;
    async fn package(
        &self,
        package_name: metadata::CleanName,
        range: Range<u64>,
    ) -> Result<RepositoryStream<Bytes>, RepositoryError>;
}

pub enum AutoRepository {
    Https(https::HttpsRepository),
    File(file::FileRepository),
}

impl AutoRepository {
    pub fn new(repository_url: &str, auth: Option<(&str, &str)>) -> Result<Self, RepositoryError> {
        if repository_url.starts_with("https://") || repository_url.starts_with("http://") {
            let mut remote_url = reqwest::Url::parse(repository_url)
                .map_err(|err| RepositoryError::InvalidUrl { reason: err.to_string() })?;
            if let Some((username, password)) = auth {
                let _ = remote_url.set_username(username);
                let _ = remote_url.set_password(Some(password));
            }
            return Ok(AutoRepository::Https(https::HttpsRepository::new(remote_url)?));
        }

        if repository_url.starts_with("file://") {
            let dir = (&repository_url["file://".len()..]).into();
            return Ok(AutoRepository::File(file::FileRepository::new(dir)));
        }

        Err(RepositoryError::InvalidUrl { reason: format!("unsupported scheme") })
    }
}

#[async_trait]
impl RemoteRepository for AutoRepository {
    async fn current_version(&self) -> Result<metadata::Current, RepositoryError> {
        match self {
            AutoRepository::Https(r) => r.current_version().await,
            AutoRepository::File(r) => r.current_version().await,
        }
    }
    async fn versions(&self) -> Result<metadata::Versions, RepositoryError> {
        match self {
            AutoRepository::Https(r) => r.versions().await,
            AutoRepository::File(r) => r.versions().await,
        }
    }
    async fn packages(&self) -> Result<metadata::Packages, RepositoryError> {
        match self {
            AutoRepository::Https(r) => r.packages().await,
            AutoRepository::File(r) => r.packages().await,
        }
    }
    async fn package_metadata(
        &self,
        package_name: metadata::CleanName,
    ) -> Result<metadata::PackageMetadata, RepositoryError> {
        match self {
            AutoRepository::Https(r) => r.package_metadata(package_name).await,
            AutoRepository::File(r) => r.package_metadata(package_name).await,
        }
    }
    async fn package(
        &self,
        package_name: metadata::CleanName,
        range: Range<u64>,
    ) -> Result<RepositoryStream<Bytes>, RepositoryError> {
        match self {
            AutoRepository::Https(r) => r.package(package_name, range).await,
            AutoRepository::File(r) => r.package(package_name, range).await,
        }
    }
}
