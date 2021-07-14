use std::borrow::Cow;
use std::fmt::Display;
use std::io::{Read, Write};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::{fs, io};

use byte_unit::Byte;
use clap::{clap_app, crate_authors, crate_description, crate_name, crate_version, ArgMatches};
use console::{style, Color, Term};
use futures::prelude::*;
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle, WeakProgressBar};
use log::{error, info, warn};
use parking_lot::RwLock;
use speedupdate::metadata::{self, CleanName, Operation};
use speedupdate::repository::{BuildOptions, CoderOptions, PackageBuilder};
use speedupdate::workspace::{UpdateOptions, Workspace};
use speedupdate::Repository;

struct Logger {
    pb: RwLock<Option<WeakProgressBar>>,
    filter: RwLock<Option<env_logger::filter::Filter>>,
}

impl Logger {
    const fn new() -> Self {
        Self { pb: parking_lot::const_rwlock(None), filter: parking_lot::const_rwlock(None) }
    }

    fn init(&self) {
        let filter = env_logger::filter::Builder::from_env("RUST_LOG").build();
        log::set_max_level(filter.filter());
        *self.filter.write() = Some(filter);
    }

    fn set_progress_bar(&self, pb: Option<WeakProgressBar>) {
        let mut pb_guard = self.pb.write();
        *pb_guard = pb;
    }

    fn matches(&self, record: &log::Record) -> bool {
        match &*self.filter.read() {
            Some(filter) => filter.matches(record),
            None => record.level() <= log::max_level(),
        }
    }
}

impl log::Log for Logger {
    fn enabled(&self, metadata: &log::Metadata) -> bool {
        match &*self.filter.read() {
            Some(filter) => filter.enabled(metadata),
            None => metadata.level() <= log::max_level(),
        }
    }

    fn log(&self, record: &log::Record) {
        if !self.matches(record) {
            return;
        }

        let level = match record.level() {
            log::Level::Error => style("  ERROR  ").bg(Color::Red).black(),
            log::Level::Warn => style("  WARN   ").bg(Color::Red).black(),
            log::Level::Info => style("  INFO   ").bg(Color::Cyan).black(),
            log::Level::Debug => style("  DEBUG  ").bg(Color::Yellow).black(),
            log::Level::Trace => style("  TRACE  ").bg(Color::Magenta).black(),
        };
        let msg =
            format!("{} {}: {}", level, record.module_path().unwrap_or_default(), record.args());

        let pb = self.pb.read();
        let pb = pb.as_ref().and_then(|weak| weak.upgrade());
        match pb {
            Some(pb) => {
                pb.println(msg);
            }
            None => {
                writeln!(io::stderr(), "{}", msg).ok();
            }
        }
    }

    fn flush(&self) {
        io::stderr().flush().ok();
    }
}

static LOGGER: Logger = Logger::new();

#[tokio::main]
async fn main() {
    LOGGER.init();
    let _ = log::set_logger(&LOGGER);

    let matches = clap_app!((crate_name!()) =>
        (setting: clap::AppSettings::SubcommandRequiredElseHelp)
        (setting: clap::AppSettings::VersionlessSubcommands)
        (setting: clap::AppSettings::DisableHelpSubcommand)
        (version: crate_version!())
        (author: crate_authors!("\n"))
        (about: crate_description!())
        (@arg repository: -r --repository "Repository path (defaults to current directory)")
        (@arg debug: -d +takes_value
            possible_value("warn")
            possible_value("info")
            possible_value("debug")
            possible_value("trace")
            default_value("info")
            "Sets the level of debugging information\n"
        )
        (@subcommand status =>
            (about: "Show the repository status (current version & stats")
        )
        (@subcommand current_version =>
            (about: "Show the repository current version")
        )
        (@subcommand log =>
            (about: "Show changelog")
            (@arg from: --from +takes_value "From revision")
            (@arg to: --to +takes_value "Up to revision")
            (@arg oneline: --oneline "Show one revision per line")
        )
        (@subcommand packages =>
            (about: "Show packages")
            (@arg from: --from +takes_value "From revision")
            (@arg to: --to +takes_value "Up to revision")
        )
        (@subcommand set_current_version =>
            (about: "Set the repository current version")
            (@arg version: +required "Version to set")
        )
        (@subcommand register_version =>
            (about: "register_package or update version details")
            (@arg version: +required "Version to add/update")
            (@arg description: --desc +takes_value "Description string")
            (@arg description_file: --("desc-file") +takes_value "utf8 file to read the description from (`-` from stdin)")
        )
        (@subcommand unregister_version =>
            (about: "Unregister version")
            (@arg version: +required "Version to add/update")
        )
        (@subcommand register_package =>
            (about: "Register or update package")
            (@arg package_metadata_name: +required "Name of the package metadata file")
        )
        (@subcommand unregister_package =>
            (about: "Unregister package")
            (@arg package_metadata_name: +required "Name of the package metadata file")
        )
        (@subcommand build_package =>
            (about: "Build package")
            (@arg version: +required "Package output version")
            (@arg source_dir: +required "Source directory the package must represent")
            (@arg from: --from +takes_value "Create a patch package from this revision")
            (@arg register: --register +takes_value "Register the built package and its version")
            (@arg compressor: --compressor -c +takes_value +multiple "Compressor options (i.e. \"brotli:6\")")
            (@arg patcher: --patcher -p +takes_value +multiple "Patcher options (i.e. \"zstd:level=3;minsize=32MB\")")
            (@arg num_threads: --("num-threads") +takes_value "Number of threads to use for building")
            (@arg build_dir: --("build-dir") +takes_value "Directory where the build process will happen")
            (@arg no_progress: --("no-progress") "Disable progress bars")
        )
    )
    .get_matches();

    match matches.value_of("debug") {
        Some("warn") => log::set_max_level(log::LevelFilter::Warn),
        Some("info") => log::set_max_level(log::LevelFilter::Info),
        Some("debug") => log::set_max_level(log::LevelFilter::Debug),
        Some("trace") => log::set_max_level(log::LevelFilter::Trace),
        Some(lvl) => {
            warn!("invalid debug level '{}', ignoring...", lvl);
        }
        None => log::set_max_level(log::LevelFilter::Info),
    };

    let repository_path = match matches.value_of("repository") {
        Some(path) => path.to_string(),
        None => std::env::current_dir().unwrap().display().to_string(),
    };
    eprintln!("repository: {}", repository_path);
    let mut repository = Repository::new(PathBuf::from(&repository_path));

    match matches.subcommand() {
        ("status", Some(matches)) => do_status(matches, &mut repository).await,
        ("current_version", Some(matches)) => do_current_version(matches, &mut repository).await,
        ("set_current_version", Some(matches)) => {
            do_set_current_version(matches, &mut repository).await
        }
        ("log", Some(matches)) => do_log(matches, &mut repository).await,
        ("register_version", Some(matches)) => do_register_version(matches, &mut repository).await,
        ("unregister_version", Some(matches)) => {
            do_unregister_version(matches, &mut repository).await
        }
        ("register_package", Some(matches)) => do_register_package(matches, &mut repository).await,
        ("unregister_package", Some(matches)) => {
            do_unregister_package(matches, &mut repository).await
        }
        ("build_package", Some(matches)) => do_build_package(matches, &mut repository).await,
        _ => unreachable!(),
    };
}
fn some_<T>(res: Option<T>, ctx: &str) -> T {
    match res {
        Some(value) => value,
        None => {
            error!("{}", ctx);
            std::process::exit(1);
        }
    }
}
fn try_<T, E: Display>(res: Result<T, E>, ctx: &str) -> T {
    match res {
        Ok(value) => value,
        Err(err) => {
            error!("unable to {}: {}", ctx, err);
            std::process::exit(1);
        }
    }
}

fn current_version(repository: &mut Repository) -> metadata::Current {
    try_(repository.current_version(), "load repository current version")
}

async fn do_status(_matches: &ArgMatches<'_>, repository: &mut Repository) {
    let current_version = current_version(repository);
    let versions = try_(repository.versions(), "load repository versions");
    let packages = try_(repository.packages(), "load repository versions");
    println!("current_version: {}", current_version.version());
    println!("versions: {}", versions.iter().count());
    println!("packages: {}", packages.iter().count());
    let size = Byte::from_bytes(packages.iter().map(|p| p.size()).sum::<u64>().into());
    println!("size: {}", size);
}

async fn do_current_version(_matches: &ArgMatches<'_>, repository: &mut Repository) {
    let current_version = current_version(repository);
    println!("{}", current_version.version());
}

async fn do_set_current_version(matches: &ArgMatches<'_>, repository: &mut Repository) {
    let version = some_(matches.value_of("version"), "no version provided");
    let version = try_(
        CleanName::new(version.to_string()),
        "convert version to clean name (i.e. [A-Za-Z0-9_.-]+)",
    );
    try_(repository.set_current_version(&version), "set current version");
}

async fn do_register_version(matches: &ArgMatches<'_>, repository: &mut Repository) {
    let version = some_(matches.value_of("version"), "no version provided");
    let version = try_(
        CleanName::new(version.to_string()),
        "convert version to clean name (i.e. [A-Za-Z0-9_.-]+)",
    );
    let description = match (matches.value_of("description"), matches.value_of("description_file"))
    {
        (None, None) => String::new(),
        (None, Some(descfile)) => try_(
            match descfile {
                "-" => {
                    let mut desc = String::new();
                    std::io::stdin().read_to_string(&mut desc).map(|_| desc)
                }
                path => std::fs::read_to_string(path),
            },
            "read description file",
        ),
        (Some(desc), None) => desc.to_string(),
        (Some(_), Some(_)) => {
            error!("--desc and --descfile are mutually exclusive");
            std::process::exit(1);
        }
    };
    let version = metadata::v1::Version { revision: version, description };
    try_(repository.register_version(&version), "register version");
}

async fn do_unregister_version(matches: &ArgMatches<'_>, repository: &mut Repository) {
    let version = some_(matches.value_of("version"), "no version provided");
    let version = try_(
        CleanName::new(version.to_string()),
        "convert version to clean name (i.e. [A-Za-Z0-9_.-]+)",
    );
    try_(repository.unregister_version(&version), "unregister version");
}

async fn do_register_package(matches: &ArgMatches<'_>, repository: &mut Repository) {
    let package_metadata_name =
        some_(matches.value_of("package_metadata_name"), "no package metadata file name provided");
    try_(repository.register_package(package_metadata_name), "register package");
}

async fn do_unregister_package(matches: &ArgMatches<'_>, repository: &mut Repository) {
    let package_metadata_name =
        some_(matches.value_of("package_metadata_name"), "no package metadata file name provided");
    try_(repository.unregister_package(package_metadata_name), "unregister package");
}

async fn do_log(matches: &ArgMatches<'_>, repository: &mut Repository) {
    let from = matches.value_of("from");
    let to = match matches.value_of("to") {
        None => current_version(repository).version().to_string(),
        Some(to) => to.to_string(),
    };
    let versions = try_(repository.versions(), "load repository versions");
    let skip_n = match from {
        Some(from) => match versions.iter().position(|v| v.revision().deref() == from) {
            Some(pos) => pos,
            None => {
                error!("unable to find starting version: {}", from);
                std::process::exit(1)
            }
        },
        None => 0,
    };
    let oneline = matches.is_present("oneline");
    for version in versions.iter().skip(skip_n) {
        if oneline {
            println!(
                "{}: {}",
                style(&version.revision()).bold(),
                version.description().lines().next().unwrap_or("")
            );
        } else {
            println!("{}", style(&version.revision()).bold());
            if !version.description().is_empty() {
                println!();
                println!("{}", version.description());
                println!();
            }
        }
        if version.revision().deref() == to {
            break;
        }
    }
}

fn op_file_name(op: Option<&dyn Operation>) -> Cow<'_, str> {
    op.and_then(|op| Path::new(op.path().deref()).file_name()).unwrap_or_default().to_string_lossy()
}

async fn do_build_package(matches: &ArgMatches<'_>, repository: &mut Repository) {
    let source_version = some_(matches.value_of("version"), "no version provided");
    let source_version = try_(
        CleanName::new(source_version.to_string()),
        "convert version to clean name (i.e. [A-Za-Z0-9_.-]+)",
    );
    let source_directory =
        PathBuf::from(some_(matches.value_of("source_dir"), "no source dir provided"));
    let build_directory = match matches.value_of("build_dir") {
        Some(build_directory) => PathBuf::from(build_directory),
        None => repository.dir().join(".build"),
    };
    let mut builder = PackageBuilder::new(build_directory, source_version, source_directory);
    if let Some(num_threads) = matches.value_of("num_threads") {
        let num_threads =
            try_(usize::from_str_radix(num_threads, 10), "convert --num-threads to integer");
        builder.set_num_threads(num_threads);
    }
    let mut options = BuildOptions::default();
    if let Some(compressors) = matches.values_of("compressor") {
        options.compressors = compressors
            .map(|s| try_(CoderOptions::from_str(s), "load compressor options"))
            .collect();
    }
    if let Some(patchers) = matches.values_of("patcher") {
        options.patchers =
            patchers.map(|s| try_(CoderOptions::from_str(s), "load patcher options")).collect();
    }
    if let Some(from) = matches.value_of("from") {
        let prev_directory = builder.build_directory.join(".from");
        try_(fs::create_dir_all(&prev_directory), "create from directory");
        let prev_version = try_(
            CleanName::new(from.to_string()),
            "convert from version to clean name (i.e. [A-Za-Z0-9_.-]+)",
        );

        let link = repository.link();
        let mut workspace = Workspace::open(&prev_directory).unwrap();
        let goal_version = Some(prev_version.clone());
        let mut update_stream = workspace.update(&link, goal_version, UpdateOptions::default());

        let state = match update_stream.next().await {
            Some(Ok(state)) => state,
            Some(Err(err)) => {
                error!("update failed: {}", err);
                std::process::exit(1)
            }
            None => unreachable!(),
        };

        let state = state.borrow();
        let progress = state.histogram.progress();

        let res = if matches.is_present("no_progress") {
            update_stream.try_for_each(|_state| future::ready(Ok(()))).await
        } else {
            let draw_target = ProgressDrawTarget::to_term(Term::buffered_stdout(), 8);
            let m = MultiProgress::with_draw_target(draw_target);
            const DL_TPL: &str =
            "Download [{wide_bar:cyan/blue}] {bytes:>8}/{total_bytes:8} ({bytes_per_sec:>10}, {eta:4}) {msg:32}";
            const IN_TPL: &str =
            "Decode   [{wide_bar:cyan/blue}] {bytes:>8}/{total_bytes:8} ({bytes_per_sec:>10}, {eta:4}) {msg:32}";
            const OU_TPL: &str =
                "Install  [{wide_bar:cyan/blue}] {bytes:>8}/{total_bytes:8} ({bytes_per_sec:>10}      ) {msg:32}";
            let sty = ProgressStyle::default_bar().progress_chars("##-");

            let dl_bytes = m.add(ProgressBar::new(state.download_bytes));
            dl_bytes.set_style(sty.clone().template(DL_TPL));
            dl_bytes.set_position(progress.downloaded_bytes);
            dl_bytes.reset_eta();

            let apply_input_bytes = m.add(ProgressBar::new(state.apply_input_bytes));
            apply_input_bytes.set_style(sty.clone().template(IN_TPL));
            apply_input_bytes.set_position(progress.applied_input_bytes);
            apply_input_bytes.reset_eta();

            let apply_output_bytes = m.add(ProgressBar::new(state.apply_output_bytes));
            apply_output_bytes.set_style(sty.clone().template(OU_TPL));
            apply_output_bytes.set_position(progress.applied_output_bytes);
            apply_output_bytes.reset_eta();

            LOGGER.set_progress_bar(Some(dl_bytes.clone().downgrade()));

            drop(state); // drop the Ref<_>

            let mp = tokio::task::spawn_blocking(move || m.join());

            let res = update_stream
                .try_for_each(|state| {
                    let state = state.borrow();
                    let progress = state.histogram.progress();
                    dl_bytes.set_position(progress.downloaded_bytes);
                    dl_bytes.set_length(state.download_bytes);
                    dl_bytes.set_message(&op_file_name(
                        state.current_step_operation(state.downloading_operation_idx),
                    ));

                    apply_input_bytes.set_position(progress.applied_input_bytes);
                    apply_input_bytes.set_length(state.apply_input_bytes);
                    apply_input_bytes.set_message(&op_file_name(
                        state.current_step_operation(state.applying_operation_idx),
                    ));

                    apply_output_bytes.set_position(progress.applied_output_bytes);
                    apply_output_bytes.set_length(state.apply_output_bytes);
                    apply_output_bytes.set_message(&format!("{:?}", state.stage));

                    future::ready(Ok(()))
                })
                .await;

            dl_bytes.finish();
            apply_input_bytes.finish();
            apply_output_bytes.finish();
            let _ = mp.await;

            res
        };

        if let Err(err) = res {
            error!("update failed: {}", err);
            std::process::exit(1)
        }
        try_(workspace.remove_metadata(), "remove update metadata");
        builder.set_previous(prev_version, prev_directory);
    }

    let mut build_stream = builder.build();

    let state = match build_stream.next().await {
        Some(Ok(state)) => state,
        Some(Err(err)) => {
            error!("build failed: {}", err);
            std::process::exit(1)
        }
        None => unreachable!(),
    };

    let state = state.borrow();
    let res = if matches.is_present("no_progress") {
        build_stream.try_for_each(|_state| future::ready(Ok(()))).await
    } else {
        let draw_target = ProgressDrawTarget::to_term(Term::buffered_stdout(), 8);
        let m = MultiProgress::with_draw_target(draw_target);
        let sty = ProgressStyle::default_bar().progress_chars("##-");
        const TPL: &str =
        "[{wide_bar:cyan/blue}] {bytes:>8}/{total_bytes:8} ({bytes_per_sec:>10}, {eta:4}) {msg:32}";

        let mut bars = state
            .workers
            .iter()
            .enumerate()
            .map(|(idx, worker)| {
                let pb = m.add(ProgressBar::new(worker.process_bytes));
                pb.set_style(sty.clone().template(&format!("{}{}", idx, TPL)));
                pb.set_position(worker.processed_bytes);
                pb.reset_eta();
                pb
            })
            .collect::<Vec<_>>();

        LOGGER.set_progress_bar(bars.get(0).map(|b| b.downgrade()));

        drop(state); // drop the Ref<_>

        let mp = tokio::task::spawn_blocking(move || m.join());

        let res = build_stream
            .try_for_each(|state| {
                let state = state.borrow();
                for (worker, bar) in state.workers.iter().zip(bars.iter_mut()) {
                    bar.set_position(worker.processed_bytes);
                    bar.set_length(worker.process_bytes);
                    bar.set_message(&*worker.task_name);
                }

                future::ready(Ok(()))
            })
            .await;

        for bar in bars {
            bar.finish();
        }
        let _ = mp.await;

        res
    };

    if let Err(err) = res {
        error!("build failed: {}", err);
        std::process::exit(1)
    }

    info!("package `{}` built", builder.package_metadata_name());

    if matches.is_present("register") {
        try_(builder.add_to_repository(repository), "register package");
    }
}
