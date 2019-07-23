extern crate brotli;
extern crate bytes;
extern crate futures;
extern crate futures_cpupool;
extern crate hyper;
extern crate hyper_tls;
#[macro_use]
extern crate log;
#[macro_use]
extern crate serde;
extern crate serde_json;
extern crate sha1;
extern crate vcdiff_rs;

mod apply;
mod download;
mod operation;
pub mod progression;
pub mod repository;
pub mod storage;
pub mod updater;
pub mod workspace;

use crate::progression::GlobalProgression;
use crate::repository::https::HttpsRepository;
use crate::repository::{RemoteRepository, RepositoryFuture};
use crate::updater::{update, Error, UpdateOptions};
use crate::workspace::Workspace;
use futures::future;
use futures::stream::Stream;
use futures::Future;
use std::path::Path;

pub const BUFFER_SIZE: usize = 65536;

pub fn update_workspace<F>(
  workspace_path: &str,
  repository_url: &str,
  auth: Option<(&str, &str)>,
  goal_version: Option<&str>,
  progress_callback: F,
) -> Result<(), Error>
where
  F: FnMut(&GlobalProgression) -> bool,
{
  info!(
    "update_workspace {} {} @ {}",
    workspace_path,
    repository_url,
    goal_version.unwrap_or("latest")
  );
  let repository = HttpsRepository::new(repository_url, auth);
  let mut workspace = Workspace::new(Path::new(workspace_path));
  workspace.load_state()?;
  let goal_version: RepositoryFuture<String> = if let Some(goal_version) = goal_version {
    Box::new(future::ok(goal_version.to_owned()))
  } else {
    Box::new(repository.current_version().and_then(|c| {
      info!("latest = {}", c.version());
      Ok(c.version().to_owned())
    }))
  };
  let mut effective_goal_version = String::new();
  let work = goal_version
    .map_err(Error::RemoteRepository)
    .and_then(|goal_version| {
      let mut progress_callback = progress_callback;
      effective_goal_version = goal_version;
      let stream = update(
        &mut workspace,
        &repository,
        &effective_goal_version,
        UpdateOptions { check: false },
      );
      stream.for_each(move |progress| {
        let progress = &*progress.borrow();
        if !progress_callback(progress) {
          Err(Error::Aborted)
        } else {
          Ok(())
        }
      })
    });
  work.wait()
}
