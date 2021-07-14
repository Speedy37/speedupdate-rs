use std::ops::Range;

use async_trait::async_trait;
use bytes::Bytes;
use futures::prelude::*;

use crate::link::{RemoteRepository, RepositoryError, RepositoryStream};
use crate::metadata;

static APP_USER_AGENT: &str = concat!(env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION"),);

pub struct HttpsRepository {
    client: reqwest::Client,
    remote_url: reqwest::Url,
}

impl HttpsRepository {
    pub fn new(remote_url: reqwest::Url) -> Result<Self, RepositoryError> {
        let client = reqwest::Client::builder().user_agent(APP_USER_AGENT).build()?;
        Ok(HttpsRepository { client, remote_url })
    }

    fn get(&self, slice: &str) -> Result<reqwest::RequestBuilder, RepositoryError> {
        let url = self
            .remote_url
            .join(slice)
            .map_err(|err| RepositoryError::InvalidUrl { reason: err.to_string() })?;
        let builder = self.client.get(url);
        Ok(builder)
    }

    async fn get_json<T>(&self, slice: &str) -> Result<T, RepositoryError>
    where
        T: for<'de> serde::Deserialize<'de>,
    {
        let request = self.get(slice)?.build()?;
        let response = self.client.execute(request).await?.error_for_status()?;
        let json = response.json().await?;
        Ok(json)
    }
}

#[async_trait]
impl RemoteRepository for HttpsRepository {
    async fn current_version(&self) -> Result<metadata::Current, RepositoryError> {
        self.get_json(metadata::Current::filename()).await
    }

    async fn versions(&self) -> Result<metadata::Versions, RepositoryError> {
        self.get_json(metadata::Versions::filename()).await
    }

    async fn packages(&self) -> Result<metadata::Packages, RepositoryError> {
        self.get_json(metadata::Packages::filename()).await
    }

    async fn package_metadata(
        &self,
        package_name: metadata::CleanName,
    ) -> Result<metadata::PackageMetadata, RepositoryError> {
        self.get_json(&package_name).await
    }

    async fn package(
        &self,
        package_name: metadata::CleanName,
        range: Range<u64>,
    ) -> Result<RepositoryStream<Bytes>, RepositoryError> {
        let request = self
            .get(&package_name)?
            .header(reqwest::header::RANGE, format!("bytes={}-{}", range.start, range.end))
            .build()?;

        let response = self.client.execute(request).await?.error_for_status()?;

        if response.status() != reqwest::StatusCode::PARTIAL_CONTENT {
            return Err(RepositoryError::HttpsNotPartialContent(response.status()));
        }

        Ok(response.bytes_stream().err_into::<RepositoryError>().boxed_local())
    }
}
