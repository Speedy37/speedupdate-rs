use std::io::Write;
use std::ops::Deref;
use std::path::Path;
use std::{io, process};

use clap::{clap_app, crate_authors, crate_description, crate_name, crate_version, ArgMatches};
use console::{style, Color, Term};
use futures::prelude::*;
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle, WeakProgressBar};
use log::{error, warn};
use parking_lot::RwLock;
use speedupdate::link::{AutoRepository, RemoteRepository};
use speedupdate::metadata::{self, v1::State, CleanName, Operation};
use speedupdate::workspace::{UpdateOptions, Workspace};

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
        (@arg workspace: -w --workspace +takes_value "Workspace directory")
        (@arg debug: -d +takes_value
            possible_value("warn")
            possible_value("info")
            possible_value("debug")
            possible_value("trace")
            default_value("info")
            "Sets the level of debugging information\n"
        )
        (@subcommand status =>
            (about: "Show the workspace status")
            (@arg repository: "Repository URL")
        )
        (@subcommand update =>
            (about: "Update workspace")
            (@arg repository: +required "Repository URL")
            (@arg to: --to +takes_value "Target revision")
            (@arg check: --check "Integrity check of all files, not just affected ones")
            (@arg no_progress: --("no-progress") "Disable progress bars")
        )
        (@subcommand check =>
            (about: "Check workspace integrity")
        )
        (@subcommand log =>
            (about: "Show changelog")
            (@arg repository: +required "Repository URL")
            (@arg from: --from +takes_value "From revision")
            (@arg to: --to +takes_value "Up to revision")
            (@arg latest: --latest +takes_value "Use repository latest revision")
            (@arg oneline: --oneline "Show one revision per line")
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

    let workspace_path = match matches.value_of("workspace") {
        Some(path) => path.to_string(),
        None => std::env::current_dir().unwrap().display().to_string(),
    };
    println!("workspace: {}", workspace_path);
    let mut workspace = match Workspace::open(&Path::new(&workspace_path)) {
        Ok(workspace) => workspace,
        Err(err) => {
            error!("unable to load workspace state: {}", err);
            process::exit(1)
        }
    };

    match matches.subcommand() {
        ("status", Some(matches)) => do_status(matches, &mut workspace).await,
        ("log", Some(matches)) => do_log(matches, &mut workspace).await,
        ("check", Some(matches)) => do_check(matches, &mut workspace).await,
        ("update", Some(matches)) => {
            let repository = arg_repository(matches).unwrap();
            do_update(matches, &mut workspace, &repository).await
        }
        _ => unreachable!(),
    };
}

fn arg_repository(matches: &ArgMatches<'_>) -> Option<AutoRepository> {
    match matches.value_of("repository") {
        Some(url) => {
            println!("repository: {}", url);
            match AutoRepository::new(url, None) {
                Ok(r) => Some(r),
                Err(err) => {
                    error!("{}", err);
                    process::exit(1)
                }
            }
        }
        None => None,
    }
}

async fn try_current_version(repository: &impl RemoteRepository) -> Option<metadata::Current> {
    match repository.current_version().await {
        Ok(current_version) => Some(current_version),
        Err(err) => {
            error!("unable to load repository current version: {}", err);
            None
        }
    }
}
async fn current_version(repository: &impl RemoteRepository) -> metadata::Current {
    match try_current_version(repository).await {
        Some(current_version) => current_version,
        None => std::process::exit(1),
    }
}

async fn do_status(matches: &ArgMatches<'_>, workspace: &mut Workspace) {
    let repository = arg_repository(matches);
    let current_version = match repository {
        Some(repository) => try_current_version(&repository).await,
        None => None,
    };
    match workspace.state() {
        State::New => {
            let latest = match current_version {
                Some(current_version) => format!(" (latest = {})", current_version.version()),
                None => String::new(),
            };
            let rev = style("NEW").bold();
            println!("status: {}{}", rev, latest);
        }
        State::Stable { version } => {
            let remote_status = match current_version {
                Some(current_version) if current_version.version() == version => {
                    style("UP to DATE").bold().green().to_string()
                }
                Some(current_version) => format!(
                    "{} (latest = {})",
                    style("OUTDATED").bold().dim(),
                    current_version.version()
                ),
                None => String::new(),
            };
            let rev = style(version).bold();
            println!("status: {}{}", rev, remote_status);
        }
        State::Corrupted { version, failures } => {
            let latest = match current_version {
                Some(current_version) => format!(" (latest = {})", current_version.version()),
                None => String::new(),
            };
            println!(
                "status: {rev} {version}{latest}",
                rev = style("CORRUPTED").bold().red(),
                version = version,
                latest = latest,
            );
            if !failures.is_empty() {
                println!("{} pending repair files:", failures.len());
                for f in failures {
                    println!(" - {path}", path = f);
                }
            }
        }
        State::Updating(d) => {
            let latest = match current_version {
                Some(current_version) => format!(" (latest = {})", current_version.version()),
                None => String::new(),
            };
            println!(
                "status: {rev} {from} → {to}{latest}",
                rev = style("UPDATING").bold().yellow(),
                from = match &d.from {
                    Some(rev) => rev,
                    None => "⊘",
                },
                to = d.to,
                latest = latest,
            );
            if !d.failures.is_empty() {
                println!("{} pending recovery files:", d.failures.len());
                for f in &d.failures {
                    println!(" - {path}", path = f);
                }
            }
        }
    }
}

async fn do_update(
    matches: &ArgMatches<'_>,
    workspace: &mut Workspace,
    repository: &impl RemoteRepository,
) {
    let goal_version = match matches.value_of("to") {
        Some(to) => match CleanName::new(to.to_string()) {
            Ok(rev) => Some(rev),
            Err(_) => {
                error!("invalid target version: {} (must match [A-Za-Z0-9_.-]+)", to);
                std::process::exit(1)
            }
        },
        None => None,
    };
    let mut update_options = UpdateOptions::default();
    update_options.check = matches.is_present("check");
    let mut stream = workspace.update(repository, goal_version, update_options);

    let state = match stream.next().await {
        Some(Ok(state)) => state,
        Some(Err(err)) => {
            error!("update failed: {}", err);
            std::process::exit(1)
        }
        None => {
            println!("UP to DATE");
            return;
        }
    };

    let state = state.borrow();
    let progress = state.histogram.progress();

    println!("Target revision: {}", state.target_revision);

    let res = if matches.is_present("no_progress") {
        drop(state); // drop the Ref<_>

        stream.try_for_each(|_state| future::ready(Ok(()))).await
    } else {
        let draw_target = ProgressDrawTarget::term(Term::buffered_stdout(), 8);
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

        let res = stream
            .try_for_each(|state| {
                let state = state.borrow();
                let progress = state.histogram.progress();
                dl_bytes.set_position(progress.downloaded_bytes);
                dl_bytes.set_length(state.download_bytes);
                dl_bytes.set_message(op_file_name(
                    state.current_step_operation(state.downloading_operation_idx),
                ));

                apply_input_bytes.set_position(progress.applied_input_bytes);
                apply_input_bytes.set_length(state.apply_input_bytes);
                apply_input_bytes.set_message(op_file_name(
                    state.current_step_operation(state.applying_operation_idx),
                ));

                apply_output_bytes.set_position(progress.applied_output_bytes);
                apply_output_bytes.set_length(state.apply_output_bytes);
                apply_output_bytes.set_message(format!("{:?}", state.stage));

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
    println!("UP to DATE");
}

fn op_file_name(op: Option<&dyn Operation>) -> String {
    op.and_then(|op| Path::new(op.path().deref()).file_name())
        .unwrap_or_default()
        .to_string_lossy()
        .into_owned()
}

async fn do_log(matches: &ArgMatches<'_>, workspace: &mut Workspace) {
    let repository = arg_repository(matches).unwrap();
    let from = matches.value_of("from");
    let to = match (matches.value_of("to"), matches.is_present("latest")) {
        (None, false) => match workspace.state() {
            State::Stable { version } => version.to_string(),
            _ => current_version(&repository).await.version().to_string(),
        },
        (Some(to), _) => to.to_string(),
        (_, true) => current_version(&repository).await.version().to_string(),
    };
    let versions = match repository.versions().await {
        Ok(versions) => versions,
        Err(err) => {
            error!("unable to load repository current version: {}", err);
            std::process::exit(1)
        }
    };
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

async fn do_check(matches: &ArgMatches<'_>, workspace: &mut Workspace) {
    let mut stream = workspace.check();
    let state = match stream.next().await {
        Some(Ok(state)) => state,
        Some(Err(err)) => {
            error!("check failed: {}", err);
            std::process::exit(1)
        }
        None => {
            println!("CHECKED");
            return;
        }
    };

    let state = state.borrow();
    let progress = state.histogram.progress();

    let res = if matches.is_present("no_progress") {
        drop(state); // drop the Ref<_>

        stream.try_for_each(|_state| future::ready(Ok(()))).await
    } else {
        let draw_target = ProgressDrawTarget::term(Term::buffered_stdout(), 8);
        let m = MultiProgress::with_draw_target(draw_target);
        const CHECK_TPL: &str =
        "Check    [{wide_bar:cyan/blue}] {bytes:>8}/{total_bytes:8} ({bytes_per_sec:>10}, {eta:4}) {msg:32}";
        let sty = ProgressStyle::default_bar().progress_chars("##-");

        let check_bytes = m.add(ProgressBar::new(state.check_bytes));
        check_bytes.set_style(sty.clone().template(CHECK_TPL));
        check_bytes.set_position(progress.checked_bytes);
        check_bytes.reset_eta();

        LOGGER.set_progress_bar(Some(check_bytes.clone().downgrade()));

        drop(state); // drop the Ref<_>

        let mp = tokio::task::spawn_blocking(move || m.join());

        let res = stream
            .try_for_each(|state| {
                let state = state.borrow();
                let progress = state.histogram.progress();
                check_bytes.set_position(progress.checked_bytes);
                check_bytes.set_length(state.check_bytes);
                check_bytes.set_message(op_file_name(state.current_operation()));

                future::ready(Ok(()))
            })
            .await;

        check_bytes.finish();
        let _ = mp.await;

        res
    };

    if let Err(err) = res {
        error!("check failed: {}", err);
        std::process::exit(1)
    }
    println!("CHECKED");
}
