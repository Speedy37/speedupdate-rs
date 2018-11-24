use storage;
use futures::{Future, IntoFuture, Stream};
use futures::future;
use hyper::header::{Authorization, ByteRangeSpec, Range as RangeHeader, UserAgent};
use hyper::{Body, Headers, Method, Request, Response, StatusCode, Uri};
use hyper::error::{Error as HyperError, UriError};
use tokio_core::reactor::Handle;
use serde_json;
use repository::{Error, RemoteRepository, RepositoryFuture, RepositoryStream};
use std::ops::Range;
use bytes::Bytes;
pub use hyper::header::Basic as BasicAuth;

pub struct HttpsRepository {
  client: ::hyper::Client<::hyper_tls::HttpsConnector<::hyper::client::HttpConnector>>,
  remote_url: String,
  headers: Headers,
}

impl HttpsRepository {
  pub fn new(
    handle: &Handle,
    remote_url: &str,
    authorization: Option<BasicAuth>,
  ) -> HttpsRepository {
    let client = ::hyper::Client::configure()
      .connector(::hyper_tls::HttpsConnector::new(1, handle).unwrap())
      .build(handle);
    let mut headers = Headers::new();
    headers.set(UserAgent::new("hyper/0.11.1"));
    if let Some(authorization) = authorization {
      headers.set(Authorization(authorization));
    }
    HttpsRepository {
      client: client,
      remote_url: String::from(remote_url),
      headers: headers,
    }
  }

  fn uri(&self, sub_path: &str) -> Result<Uri, UriError> {
    let mut url = self.remote_url.clone();
    if !url.ends_with("/") {
      url.push_str("/");
    }
    url.push_str(sub_path);
    url.parse()
  }

  fn get(&self, sub_path: &str, headers: Option<Headers>) -> RepositoryFuture<Response> {
    match self.uri(sub_path) {
      Ok(url) => {
        let mut req = Request::new(Method::Get, url);
        req.headers_mut().extend(self.headers.iter());
        if let Some(headers) = headers {
          req.headers_mut().extend(headers.iter());
        }
        let res = self.client.request(req).map_err(|e| Error::Hyper(e));
        Box::new(res)
      }
      Err(why) => Box::new(Err(Error::Hyper(HyperError::from(why))).into_future()),
    }
  }
}

fn is_statuscode_ok(res: Response) -> Result<Body, Error> {
  if res.status() == StatusCode::Ok {
    Ok(res.body())
  } else {
    Err(Error::StatusCode(res.status()))
  }
}

impl RemoteRepository for HttpsRepository {
  fn current_version(&self) -> RepositoryFuture<storage::Current> {
    let body = self
      .get("current", None)
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
      .get("versions", None)
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
      .get("packages", None)
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
      .get(package_name_metadata, None)
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
    let mut headers = Headers::new();
    headers.set(RangeHeader::Bytes(vec![
      ByteRangeSpec::FromTo(range.start, range.end),
    ]));
    let body = self
      .get(package_name, Some(headers))
      .and_then(move |res| match res.status() {
        StatusCode::PartialContent => Ok(res.body()),
        status => Err(Error::StatusCode(status)),
      });
    let chunks = body.and_then(|body| future::ok(body.map(Bytes::from).map_err(Error::Hyper)));
    let chunks = chunks.flatten_stream();
    Box::new(chunks)
  }
}
