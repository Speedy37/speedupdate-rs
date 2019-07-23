pub mod https;
pub mod local;

use crate::storage;
use bytes::Bytes;
use futures::{Future, Stream};
use hyper::error::Error as HyperError;
use hyper::StatusCode;
use serde_json::error::Error as JsonError;
use std::fmt;
use std::io;
use std::ops::Range;
use std::path::PathBuf;

#[derive(Debug)]
pub enum Error {
  IoError(io::Error),
  Hyper(HyperError),
  Http(hyper::http::Error),
  Json(JsonError),
  StatusCode(StatusCode),
}

impl fmt::Display for Error {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    match self {
      &Error::IoError(ref io_error) => io_error.fmt(f),
      &Error::Hyper(ref hyper_error) => hyper_error.fmt(f),
      &Error::Http(ref http_error) => http_error.fmt(f),
      &Error::Json(ref json_error) => json_error.fmt(f),
      &Error::StatusCode(status_code) => write!(f, "{}", status_code),
    }
  }
}

pub type RepositoryFuture<Item> = Box<Future<Item = Item, Error = Error>>;
pub type RepositoryStream<Item> = Box<Stream<Item = Item, Error = Error>>;
pub trait RemoteRepository {
  fn current_version(&self) -> RepositoryFuture<storage::Current>;
  fn versions(&self) -> RepositoryFuture<storage::Versions>;
  fn packages(&self) -> RepositoryFuture<storage::Packages>;
  fn package_metadata(&self, package_name: &str) -> RepositoryFuture<storage::PackageMetadata>;
  fn package(&self, package_name: &str, range: Range<u64>) -> RepositoryStream<Bytes>;
}

pub enum AutoRepository {
  Https(https::HttpsRepository),
  File(local::LocalRepository),
}

impl AutoRepository {
  pub fn new(repository_url: &str, auth: Option<(&str, &str)>) -> Option<Self> {
    if repository_url.starts_with("https://") {
      Some(AutoRepository::Https(https::HttpsRepository::new(
        repository_url,
        auth,
      )))
    } else if repository_url.starts_with("file://") {
      Some(AutoRepository::File(local::LocalRepository::new(
        (&repository_url["file://".len()..]).into(),
      )))
    } else {
      None
    }
  }
}

impl RemoteRepository for AutoRepository {
  fn current_version(&self) -> RepositoryFuture<storage::Current> {
    match self {
      AutoRepository::Https(r) => r.current_version(),
      AutoRepository::File(r) => r.current_version(),
    }
  }
  fn versions(&self) -> RepositoryFuture<storage::Versions> {
    match self {
      AutoRepository::Https(r) => r.versions(),
      AutoRepository::File(r) => r.versions(),
    }
  }
  fn packages(&self) -> RepositoryFuture<storage::Packages> {
    match self {
      AutoRepository::Https(r) => r.packages(),
      AutoRepository::File(r) => r.packages(),
    }
  }
  fn package_metadata(&self, package_name: &str) -> RepositoryFuture<storage::PackageMetadata> {
    match self {
      AutoRepository::Https(r) => r.package_metadata(package_name),
      AutoRepository::File(r) => r.package_metadata(package_name),
    }
  }
  fn package(&self, package_name: &str, range: Range<u64>) -> RepositoryStream<Bytes> {
    match self {
      AutoRepository::Https(r) => r.package(package_name, range),
      AutoRepository::File(r) => r.package(package_name, range),
    }
  }
}
