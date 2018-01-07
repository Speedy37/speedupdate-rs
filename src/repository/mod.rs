pub mod https;
pub mod local;

use storage;
use std::fmt;
use futures::{Future, Stream};
use hyper::StatusCode;
use hyper::error::Error as HyperError;
use serde_json::error::Error as JsonError;
use std::ops::Range;
use std::io;
use bytes::Bytes;

#[derive(Debug)]
pub enum Error {
  IoError(io::Error),
  Hyper(HyperError),
  Json(JsonError),
  StatusCode(StatusCode),
}

impl fmt::Display for Error {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    match self {
      &Error::IoError(ref io_error) => io_error.fmt(f),
      &Error::Hyper(ref hyper_error) => hyper_error.fmt(f),
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
