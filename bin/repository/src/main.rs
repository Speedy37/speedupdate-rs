extern crate brotli;
extern crate clap;
extern crate env_logger;
extern crate futures;
extern crate futures_cpupool;
#[macro_use]
extern crate log;
extern crate serde;
extern crate serde_json;
extern crate sha1;
extern crate tokio_core;
extern crate updater;

mod repository;

use clap::{crate_authors, crate_name, crate_version, App, Arg, SubCommand};
use std::fs;
use std::path::Path;

use repository::Repository;

fn main() -> Result<(), ()> {
  env_logger::init();

  let app_m = App::new(crate_name!())
    .version(crate_version!())
    .author(crate_authors!("\n"))
    .about("Manages an update repository")
    .subcommand(
      SubCommand::with_name("init")
        .about("init a new repository")
        .arg(
          Arg::with_name("PATH")
            .help("Repository path")
            .required(true),
        ),
    ).subcommand(
      SubCommand::with_name("add-package")
        .about("add package to repository")
        .arg(
          Arg::with_name("PATH")
            .help("Repository path")
            .required(true),
        ).arg(Arg::with_name("DATA").help("Path to pack").required(true))
        .arg(
          Arg::with_name("VERSION")
            .help("Pack version")
            .required(true),
        ),
    ).get_matches();

  match app_m.subcommand() {
    ("init", Some(sub_m)) => {
      let path = sub_m.value_of("PATH").expect("Repository path");
      repository_init(path)
    }
    ("add-package", Some(sub_m)) => {
      let path = sub_m.value_of("PATH").expect("Repository path");
      let data = sub_m.value_of("DATA").expect("Path to pack");
      let version = sub_m.value_of("VERSION").expect("Path version");
      repository_add_package(path, data, version)
    }
    (cmd, _) => Err(format!("unknown command: {}", cmd)),
  }.map(|msg| {
    println!("{}", msg);
    ()
  }).map_err(|msg| {
    println!("{}", msg);
    ()
  })
}

fn repository_init(path: &str) -> Result<String, String> {
  let repository_dir = Path::new(path);
  fs::create_dir_all(&repository_dir)
    .map_err(|err| format!("unable to create repository directory: {}", err))?;
  let mut repository = Repository::new(repository_dir.to_owned());
  repository
    .init()
    .map_err(|err| format!("unable to initialize repository: {}", err))?;
  Ok(format!("repository initialized"))
}

fn repository_add_package(path: &str, data: &str, version: &str) -> Result<String, String> {
  let repository_dir = Path::new(path);
  let mut repository = Repository::new(repository_dir.to_owned());
  let build_dir = repository_dir.join(".build");
  repository
    .add_package(&build_dir, Path::new(data), version, "", None)
    .map_err(|err| format!("unable to add-package: {}", err))?;
  Ok(format!("package added"))
}
