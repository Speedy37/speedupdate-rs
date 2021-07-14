use std::ops::Range;
use std::path::PathBuf;

use async_trait::async_trait;
use bytes::Bytes;
use futures::prelude::*;
use serde_json;
use tokio::io::AsyncSeekExt;

use crate::link::{RemoteRepository, RepositoryError, RepositoryStream};
use crate::metadata;

pub struct FileRepository {
    dir: PathBuf,
}

impl FileRepository {
    pub fn new(dir: PathBuf) -> FileRepository {
        FileRepository { dir }
    }

    async fn get<T>(&self, file_name: &str) -> Result<T, RepositoryError>
    where
        T: for<'de> serde::Deserialize<'de>,
    {
        let path = self.dir.join(&file_name);
        let raw = tokio::fs::read(&path).await.map_err(|err| RepositoryError::file(&path, err))?;
        let decoded =
            serde_json::from_slice::<T>(&raw).map_err(|err| RepositoryError::json(&path, err))?;
        Ok(decoded)
    }
}

#[async_trait]
impl RemoteRepository for FileRepository {
    async fn current_version(&self) -> Result<metadata::Current, RepositoryError> {
        self.get(metadata::Current::filename()).await
    }

    async fn versions(&self) -> Result<metadata::Versions, RepositoryError> {
        self.get(metadata::Versions::filename()).await
    }

    async fn packages(&self) -> Result<metadata::Packages, RepositoryError> {
        self.get(metadata::Packages::filename()).await
    }

    async fn package_metadata(
        &self,
        package_name: metadata::CleanName,
    ) -> Result<metadata::PackageMetadata, RepositoryError> {
        self.get(&package_name).await
    }

    async fn package(
        &self,
        package_name: metadata::CleanName,
        range: Range<u64>,
    ) -> Result<RepositoryStream<Bytes>, RepositoryError> {
        let path = self.dir.join(&package_name);
        let mut file =
            tokio::fs::File::open(&path).map_err(|err| RepositoryError::file(&path, err)).await?;

        let new_pos = file
            .seek(tokio::io::SeekFrom::Start(range.start))
            .map_err(|err| RepositoryError::file(&path, err))
            .await?;
        if new_pos != range.start {
            return Err(RepositoryError::file(
                &path,
                std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "failed to seek at the correct position",
                ),
            ));
        }

        let stream = tokio_util::io::ReaderStream::new(file)
            .map_err(move |err| RepositoryError::file(&path, err))
            .boxed_local();

        Ok(stream)
    }
}
