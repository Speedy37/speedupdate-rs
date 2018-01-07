use storage;
use futures::{future, stream, Async, Future, Poll, Stream};
use std::path::PathBuf;
use std::io::{self, Read, Seek, SeekFrom};
use std::cmp;
use std::fs;
use serde_json;
use repository::{Error, RemoteRepository, RepositoryFuture, RepositoryStream};
use std::ops::Range;
use bytes::{Bytes, BytesMut};
use BUFFER_SIZE;

pub struct LocalRepository {
  dir: PathBuf,
}

impl LocalRepository {
  pub fn new(dir: PathBuf) -> LocalRepository {
    LocalRepository { dir }
  }

  fn get(&self, file_name: &str) -> future::FutureResult<fs::File, Error> {
    future::result(
      fs::OpenOptions::new()
        .read(true)
        .open(self.dir.join(file_name))
        .map_err(Error::IoError),
    )
  }
}

impl RemoteRepository for LocalRepository {
  fn current_version(&self) -> RepositoryFuture<storage::Current> {
    let json = self
      .get("current")
      .and_then(|file| serde_json::from_reader::<_, storage::Current>(file).map_err(Error::Json));
    Box::new(json)
  }

  fn versions(&self) -> RepositoryFuture<storage::Versions> {
    let json = self
      .get("versions")
      .and_then(|file| serde_json::from_reader::<_, storage::Versions>(file).map_err(Error::Json));
    Box::new(json)
  }
  fn packages(&self) -> RepositoryFuture<storage::Packages> {
    let json = self
      .get("packages")
      .and_then(|file| serde_json::from_reader::<_, storage::Packages>(file).map_err(Error::Json));
    Box::new(json)
  }
  fn package_metadata(
    &self,
    package_name_metadata: &str,
  ) -> RepositoryFuture<storage::PackageMetadata> {
    let json = self.get(package_name_metadata).and_then(|file| {
      serde_json::from_reader::<_, storage::PackageMetadata>(file).map_err(Error::Json)
    });
    Box::new(json)
  }

  fn package(&self, package_name: &str, range: Range<u64>) -> RepositoryStream<Bytes> {
    let stream = self
      .get(package_name)
      .and_then(move |mut file| {
        file
          .seek(SeekFrom::Start(range.start))
          .map_err(Error::IoError)?;
        let mut remaining = range.end - range.start;
        let stream = stream::poll_fn(move || -> Poll<Option<Bytes>, io::Error> {
          let mut buf = BytesMut::with_capacity(BUFFER_SIZE);
          let read = cmp::min(buf.len() as u64, remaining);
          remaining -= read;
          file.read(&mut buf[0..read as usize])?;
          Ok(Async::Ready(Some(buf.freeze())))
        }).map_err(Error::IoError);
        Ok(stream)
      })
      .flatten_stream();
    Box::new(stream)
  }
}
