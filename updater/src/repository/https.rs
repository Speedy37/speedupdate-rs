use crate::repository::{Error, RemoteRepository, RepositoryFuture, RepositoryStream};
use crate::storage;
use bytes::Bytes;
use futures::future;
use futures::{Future, IntoFuture, Stream};
use hyper::header;
use hyper::{client::HttpConnector, Client};
use hyper::{Body, Request, Response, StatusCode};
use hyper_tls::HttpsConnector;
use serde_json;
use std::ops::Range;

pub struct HttpsRepository {
  client: Client<HttpsConnector<HttpConnector>>,
  remote_url: String,
  authorization: Option<String>,
}

impl HttpsRepository {
  pub fn new(remote_url: &str, authorization: Option<(&str, &str)>) -> HttpsRepository {
    let https = HttpsConnector::new(1).expect("TLS initialization failed");
    let client = Client::builder().build::<_, Body>(https);
    let authorization = authorization.map(|(username, password)| {
      format!(
        "Basic {}",
        base64::encode(&format!("{}:{}", username, password))
      )
    });
    HttpsRepository {
      client,
      remote_url: remote_url.to_string(),
      authorization,
    }
  }

  fn uri(&self, sub_path: &str) -> String {
    let mut url = self.remote_url.clone();
    if !url.ends_with("/") {
      url.push_str("/");
    }
    url.push_str(sub_path);
    url
  }

  fn get(
    &self,
    sub_path: &str,
    headers_it: impl Iterator<Item = (header::HeaderName, String)>,
  ) -> impl Future<Item = Response<Body>, Error = Error> {
    (|| {
      let mut builder = Request::get(self.uri(sub_path));
      builder.header(header::USER_AGENT, "hyper/0.11.1");
      if let Some(authorization) = self.authorization.as_ref() {
        builder.header(header::AUTHORIZATION, authorization.as_str());
      }
      for (name, val) in headers_it {
        builder.header(name, val);
      }

      builder.body(Body::default())
    })()
    .map_err(Error::Http)
    .map(|req| self.client.request(req).map_err(Error::Hyper))
    .into_future()
    .flatten()
  }
}

fn is_statuscode_ok(res: Response<Body>) -> Result<Body, Error> {
  if res.status() == StatusCode::OK {
    Ok(res.into_body())
  } else {
    Err(Error::StatusCode(res.status()))
  }
}

impl RemoteRepository for HttpsRepository {
  fn current_version(&self) -> RepositoryFuture<storage::Current> {
    let body = self
      .get("current", std::iter::empty())
      .and_then(|res| is_statuscode_ok(res));
    let json = body.and_then(|body| {
      body
        .concat2()
        .map_err(|e| Error::Hyper(e))
        .and_then(|body| {
          serde_json::from_slice::<storage::Current>(&body).map_err(|e| Error::Json(e))
        })
    });
    Box::new(json)
  }

  fn versions(&self) -> RepositoryFuture<storage::Versions> {
    let body = self
      .get("versions", std::iter::empty())
      .and_then(|res| is_statuscode_ok(res));
    let json = body.and_then(|body| {
      body
        .concat2()
        .map_err(|e| Error::Hyper(e))
        .and_then(|body| {
          serde_json::from_slice::<storage::Versions>(&body).map_err(|e| Error::Json(e))
        })
    });
    Box::new(json)
  }
  fn packages(&self) -> RepositoryFuture<storage::Packages> {
    let body = self
      .get("packages", std::iter::empty())
      .and_then(|res| is_statuscode_ok(res));
    let json = body.and_then(|body| {
      body
        .concat2()
        .map_err(|e| Error::Hyper(e))
        .and_then(|body| {
          serde_json::from_slice::<storage::Packages>(&body).map_err(|e| Error::Json(e))
        })
    });
    Box::new(json)
  }
  fn package_metadata(
    &self,
    package_name_metadata: &str,
  ) -> RepositoryFuture<storage::PackageMetadata> {
    let body = self
      .get(package_name_metadata, std::iter::empty())
      .and_then(|res| is_statuscode_ok(res));
    let json = body.and_then(|body| {
      body
        .concat2()
        .map_err(|e| Error::Hyper(e))
        .and_then(|body| {
          serde_json::from_slice::<storage::PackageMetadata>(&body).map_err(|e| Error::Json(e))
        })
    });
    Box::new(json)
  }

  fn package(&self, package_name: &str, range: Range<u64>) -> RepositoryStream<Bytes> {
    let body = self
      .get(
        package_name,
        std::iter::once((
          header::RANGE,
          format!("bytes={}-{}", range.start, range.end),
        )),
      )
      .and_then(move |res| match res.status() {
        StatusCode::PARTIAL_CONTENT => Ok(res.into_body()),
        status => Err(Error::StatusCode(status)),
      });
    let chunks = body.and_then(|body| future::ok(body.map(Bytes::from).map_err(Error::Hyper)));
    let chunks = chunks.flatten_stream();
    Box::new(chunks)
  }
}
