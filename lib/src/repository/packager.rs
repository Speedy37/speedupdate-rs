use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::{fmt, fs};

use futures::prelude::*;
use io::BUFFER_SIZE;
use tracing::{debug, error, instrument, span, Level};

use super::progress::{BuildProgress, BuildStage, BuildWorkerProgress, SharedBuildProgress};
use crate::codecs::{CheckCoder, CoderOptions};
use crate::metadata::{self, CleanName, CleanPath, Operation, Package, Sha1Hash};
use crate::sync::watch_progress;
use crate::{io, Repository};

/// Build a new repository package
pub struct PackageBuilder {
    /// Directory where the build process will happen
    pub build_directory: PathBuf,
    /// Package output revision
    pub source_version: CleanName,
    /// Source directory the package output must match
    pub source_directory: PathBuf,
    /// Previous version revision and directory if targeting a patch package
    pub previous: Option<(CleanName, PathBuf)>,
    /// Number of threads to use for building
    pub num_threads: NonZeroUsize,
    /// Shared build options
    pub options: Arc<BuildOptions>,
}

#[derive(Debug)]
pub enum BuildError {
    BuildTaskList(io::Error),
    JoinError(tokio::task::JoinError),
    TaskError { name: Arc<str>, err: io::Error },
    PackageCreateError { path: Box<str>, err: io::Error },
    MetaCreateError { path: Box<str>, err: io::Error },
    OpenOperationError { path: Box<str>, err: io::Error },
    CopyOperationError { path: Box<str>, err: io::Error },
    RmOperationError { path: Box<str>, err: io::Error },
}

impl fmt::Display for BuildError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            BuildError::BuildTaskList(err) => write!(f, "build task list error: {}", err),
            BuildError::JoinError(err) => write!(f, "failed to join task: {}", err),
            BuildError::TaskError { name, err } => write!(f, "task {} failed: {}", name, err),
            BuildError::PackageCreateError { path, err } => {
                write!(f, "failed to create package file {}: {}", path, err)
            }
            BuildError::MetaCreateError { path, err } => {
                write!(f, "failed to create meta file {}: {}", path, err)
            }
            BuildError::OpenOperationError { path, err } => {
                write!(f, "failed to open operation file {}: {}", path, err)
            }
            BuildError::CopyOperationError { path, err } => {
                write!(f, "failed to copy operation content {}: {}", path, err)
            }
            BuildError::RmOperationError { path, err } => {
                write!(f, "failed to remove operation file {}: {}", path, err)
            }
        }
    }
}

impl std::error::Error for BuildError {}

pub type BuildProgressStream<'a> =
    Pin<Box<dyn Stream<Item = Result<SharedBuildProgress, BuildError>> + 'a>>;

impl PackageBuilder {
    pub fn new(
        build_directory: PathBuf,
        source_version: CleanName,
        source_directory: PathBuf,
    ) -> Self {
        Self {
            build_directory,
            source_version,
            source_directory,
            previous: None,
            num_threads: NonZeroUsize::new(num_cpus::get()).expect(">= 1"),
            options: Arc::new(BuildOptions::default()),
        }
    }

    pub fn set_previous(&mut self, prev_version: CleanName, prev_directory: PathBuf) {
        self.previous = Some((prev_version, prev_directory));
    }

    pub fn set_num_threads(&mut self, num_threads: usize) {
        self.num_threads = NonZeroUsize::new(num_threads.max(1)).expect(">= 1");
    }

    pub fn set_options(&mut self, options: BuildOptions) {
        self.options = Arc::new(options);
    }

    fn package_new(&self) -> metadata::v1::Package {
        metadata::v1::Package {
            from: self.previous.as_ref().map(|(rev, _)| rev.clone()),
            to: self.source_version.to_owned(),
            size: 0,
        }
    }

    pub fn add_to_repository(&self, repository: &mut Repository) -> io::Result<()> {
        let package = self.package_new();
        let package_data_name = package.package_data_name();
        let package_metadata_name = package.package_metadata_name();
        let built_data_path = self.build_directory.join(&package_data_name);
        let built_metadata_path = self.build_directory.join(&package_metadata_name);
        let repo_data_path = repository.dir.join(&package_data_name);
        let repo_metadata_path = repository.dir.join(&package_metadata_name);

        io::assert_is_file_eq(&built_data_path, true, "built data file")?;
        io::assert_is_file_eq(&built_metadata_path, true, "built metadata file")?;
        io::assert_is_file_eq(&repo_data_path, false, "repository data file")?;
        io::assert_is_file_eq(&repo_metadata_path, false, "repository metadata file")?;

        fs::rename(&built_data_path, &repo_data_path)?;
        let res2 = fs::rename(&built_metadata_path, &repo_metadata_path);
        if res2.is_err() {
            let _ = fs::rename(repo_data_path, built_data_path);
        }
        res2?;

        repository.register_package(&package_metadata_name)?;

        Ok(())
    }

    pub fn package_metadata_name(&self) -> CleanName {
        self.package_new().package_metadata_name()
    }

    pub fn package_data_name(&self) -> CleanName {
        self.package_new().package_data_name()
    }

    pub fn package_data_path(&self) -> PathBuf {
        self.build_directory.join(self.package_data_name())
    }

    pub fn package_metadata_path(&self) -> PathBuf {
        self.build_directory.join(self.package_metadata_name())
    }

    async fn execute(
        &self,
        txs: Vec<watch_progress::Sender<(u64, BuildWorkerProgress)>>,
    ) -> Result<(), BuildError> {
        let txs = Arc::new(parking_lot::Mutex::new(txs));

        let build_directory = self.build_directory.clone();
        let source_directory = self.source_directory.clone();
        let previous = self.previous.clone();
        let options = self.options.clone();
        let tasks = tokio::task::spawn_blocking(move || -> Result<_, BuildError> {
            let mut task_builder = BuildTaskBuilder { tasks: Vec::new() };
            fs::create_dir_all(&build_directory).map_err(BuildError::BuildTaskList)?;
            task_builder
                .push_dir(
                    &options,
                    &build_directory,
                    Some(&source_directory),
                    previous.as_ref().map(|(_version, path)| path.as_path()),
                    Path::new(""),
                )
                .map_err(BuildError::BuildTaskList)?;
            Ok(task_builder.tasks)
        })
        .map_err(BuildError::JoinError)
        .await??;

        let options = self.options.clone();
        let mut ops_groups: Vec<(usize, BuiltOperation)> =
            stream::iter(tasks.into_iter().enumerate())
                .map(|(i, task)| {
                    let tx = { txs.lock().pop().expect("one tx per worker") };
                    let mut ctx = BuildTaskCtx {
                        options: options.clone(),
                        progress: BuildWorkerProgress {
                            task_name: Arc::from(String::new()),
                            processed_bytes: 0,
                            process_bytes: 0,
                        },
                        tx,
                    };
                    let txs = txs.clone();
                    tokio::task::spawn_blocking(move || -> Result<_, BuildError> {
                        let op = task(&mut ctx)?;
                        txs.lock().push(ctx.tx);
                        Ok((i, op))
                    })
                })
                .buffer_unordered(self.num_threads.get())
                .map(|res| match res {
                    Ok(v) => v,
                    Err(err) => Err(BuildError::JoinError(err)),
                })
                .try_collect()
                .await?;

        let mut package_v1 = self.package_new();
        let data_path = self.build_directory.join(package_v1.package_data_name());
        let metadata_path = self.build_directory.join(package_v1.package_metadata_name());
        tokio::task::spawn_blocking(move || -> Result<_, BuildError> {
            let mut txs = txs.lock();
            let tx = txs.pop().expect("a least one tx");
            let mut ctx = BuildTaskCtx {
                options: options.clone(),
                progress: BuildWorkerProgress {
                    task_name: Arc::from(String::new()),
                    processed_bytes: 0,
                    process_bytes: 0,
                },
                tx,
            };
            txs.clear();
            drop(txs);

            ops_groups.sort_by_key(|(i, _built_op)| *i);

            ctx.set_task_name("write package".into());
            ctx.set_len(
                ops_groups.iter().map(|(_, built_op)| built_op.operation.data_size()).sum(),
            );

            let path = || data_path.display().to_string().into_boxed_str();
            let mut package_file = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&data_path)
                .map_err(|err| BuildError::PackageCreateError { path: path(), err })?;
            let mut operations = Vec::new();
            for (_i, mut built_op) in ops_groups {
                if let Some(data_path) = built_op.data_path {
                    debug!(
                        "merging {}(size: {}) data at {}",
                        built_op.operation.path(),
                        built_op.operation.data_size(),
                        package_v1.size,
                    );
                    built_op.operation.set_data_offset(package_v1.size);
                    package_v1.size += built_op.operation.data_size();
                    let path = || data_path.display().to_string().into_boxed_str();
                    let mut data_file = fs::File::open(&data_path)
                        .map_err(|err| BuildError::OpenOperationError { path: path(), err })?;
                    let mut buffer = [0u8; io::BUFFER_SIZE];
                    let mut copied = 0u64;
                    loop {
                        let read = data_file
                            .read(&mut buffer)
                            .map_err(|err| BuildError::CopyOperationError { path: path(), err })?;
                        if read == 0 {
                            break;
                        }
                        package_file
                            .write_all(&buffer[..read])
                            .map_err(|err| BuildError::CopyOperationError { path: path(), err })?;
                        ctx.inc(read as u64);
                        copied += read as u64;
                    }
                    io::assert_eq(
                        copied,
                        built_op.operation.data_size(),
                        "copied data file into package size",
                    )
                    .map_err(|err| BuildError::CopyOperationError { path: path(), err })?;
                    io::remove_file(&data_path)
                        .map_err(|err| BuildError::RmOperationError { path: path(), err })?;
                }

                operations.push(built_op.operation);
            }

            package_file
                .flush()
                .map_err(|err| BuildError::PackageCreateError { path: path(), err })?;

            let package_metadata_v1 =
                metadata::PackageMetadata::V1 { package: package_v1.clone(), operations };

            {
                let path = || metadata_path.display().to_string().into_boxed_str();
                let meta_err = |err| BuildError::MetaCreateError { path: path(), err };
                let mut metadata_file = fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&metadata_path)
                    .map_err(meta_err)?;
                serde_json::to_writer_pretty(&mut metadata_file, &package_metadata_v1)
                    .map_err(|err| meta_err(err.into()))?;
                metadata_file.flush().map_err(meta_err)?;
            }

            Ok(())
        })
        .map_err(BuildError::JoinError)
        .await??;

        Ok(())
    }

    /// Starts building packages and returns a progression stream
    ///
    /// To cancel the building process, stop polling the stream
    pub fn build(&self) -> BuildProgressStream<'_> {
        let progression = SharedBuildProgress::new(BuildProgress {
            workers: (0..self.num_threads.get())
                .map(|_| BuildWorkerProgress {
                    task_name: Arc::from(String::new()),
                    processed_bytes: 0,
                    process_bytes: 0,
                })
                .collect(),
            stage: BuildStage::BuildingOperations,
            processed_bytes: 0,
            process_bytes: 0,
        });
        let mut txs = Vec::new();
        let mut rxs = Vec::new();
        for thread_idx in 0..self.num_threads.get() {
            let (tx, rx) = watch_progress::channel::<(u64, BuildWorkerProgress)>();
            txs.push(tx);
            let progression = progression.clone();
            rxs.push(rx.map(move |(delta, progress)| {
                let mut p = progression.borrow_mut();
                p.processed_bytes += delta;
                p.workers[thread_idx] = progress;
                drop(p);
                Ok(progression.clone())
            }));
        }

        let rx_stream = stream::select_all(rxs);
        let w_stream = self.execute(txs).map(|_| stream::empty()).flatten_stream();

        stream::select(rx_stream, w_stream).boxed_local()
    }
}

fn err(msg: &str) -> io::Error {
    error!("{}", msg);
    io::Error::new(io::ErrorKind::Other, msg)
}

#[cfg(unix)]
fn is_exe(_filename: &str, metadata: &fs::Metadata) -> bool {
    use std::os::unix::fs::PermissionsExt;
    return metadata.permissions().mode() & 0o111 > 0;
}

#[cfg(not(unix))]
fn is_exe(filename: &str, _metadata: &fs::Metadata) -> bool {
    return filename.ends_with(".exe");
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileType {
    None,
    Dir,
    File,
    Exe,
}

impl FileType {
    fn new(filename: &str, metadata: &fs::Metadata) -> io::Result<Self> {
        match metadata.file_type() {
            t if t.is_dir() => Ok(FileType::Dir),
            t if t.is_file() => {
                if is_exe(filename, metadata) {
                    Ok(FileType::Exe)
                } else {
                    Ok(FileType::File)
                }
            }
            t => Err(err(&format!("unsupported file type {:?}", t))),
        }
    }

    fn is_dir(self) -> bool {
        matches!(self, FileType::Dir)
    }

    fn is_file(self) -> bool {
        matches!(self, FileType::File | FileType::Exe)
    }

    fn is_exe(self) -> bool {
        matches!(self, FileType::Exe)
    }
}

impl Default for FileType {
    fn default() -> Self {
        FileType::None
    }
}

#[derive(Debug, Default)]
pub struct FileState {
    pre: FileType,
    src: FileType,
}

fn ordered_dir_list(
    vec: &mut BTreeMap<String, FileState>,
    dir: Option<&Path>,
    is_pre: bool,
) -> io::Result<()> {
    if let Some(dir) = dir {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let filename = entry.file_name();
            let filename = filename
                .to_str()
                .ok_or_else(|| err(&format!("weird characters in filename {:?}", filename)))?
                .to_string();
            let metadata = entry.metadata()?;
            let filetype = FileType::new(&filename, &metadata)?;
            let entry = vec.entry(filename).or_default();
            let v = if is_pre { &mut entry.pre } else { &mut entry.src };
            *v = filetype;
        }
    }
    Ok(())
}

struct BuiltOperation {
    pub operation: metadata::v1::Operation,
    pub data_path: Option<PathBuf>,
}

impl BuiltOperation {
    fn no_data(operation: metadata::v1::Operation) -> Self {
        Self { operation, data_path: None }
    }

    fn with_data(data_path: PathBuf, operation: metadata::v1::Operation) -> Self {
        Self { operation, data_path: Some(data_path) }
    }
}

/// Build package options (compressors, patchers, ...)
pub struct BuildOptions {
    pub compressors: Vec<CoderOptions>,
    pub patchers: Vec<CoderOptions>,
}

impl BuildOptions {
    pub fn raw() -> Self {
        Self {
            compressors: vec![CoderOptions::new("raw".to_string())],
            patchers: vec![CoderOptions::new("raw".to_string())],
        }
    }
}

impl Default for BuildOptions {
    fn default() -> Self {
        Self {
            compressors: vec![
                #[cfg(feature = "brotli")]
                CoderOptions::new("brotli".to_string()),
                #[cfg(feature = "zstd")]
                CoderOptions::new("zstd".to_string()),
                CoderOptions::new("raw".to_string()),
            ],
            patchers: vec![
                #[cfg(feature = "zstd")]
                CoderOptions::new("zstd".to_string()),
                CoderOptions::new("raw".to_string()),
            ],
        }
    }
}

struct BuildTaskCtx {
    options: Arc<BuildOptions>,
    progress: BuildWorkerProgress,
    tx: crate::sync::watch_progress::Sender<(u64, BuildWorkerProgress)>,
}

impl BuildTaskCtx {
    fn set_task_name(&mut self, task_name: Arc<str>) {
        self.progress.task_name = task_name;
        self.inc(0);
    }

    fn set_len(&mut self, len: u64) {
        self.progress.processed_bytes = 0;
        self.progress.process_bytes = len;
        self.inc(0);
    }

    fn inc(&mut self, len: u64) {
        self.progress.processed_bytes += len;
        let progress = self.progress.clone();
        self.tx.send_acc(move |current| match current {
            None => (len, progress),
            Some((delta, _)) => (delta + len, progress),
        });
    }
}

struct BuildTaskBuilder {
    tasks: Vec<Box<dyn FnOnce(&mut BuildTaskCtx) -> Result<BuiltOperation, BuildError> + Send>>,
}

impl BuildTaskBuilder {
    fn push<T>(&mut self, name: &str, task: T)
    where
        T: FnOnce(&mut BuildTaskCtx) -> Result<BuiltOperation, io::Error> + Send,
        T: Send + 'static,
    {
        let name: Arc<str> = Arc::from(name);
        self.tasks.push(Box::new(move |ctx| {
            let task_name: &str = &name;
            let span = span!(Level::INFO, "build task", task_name = task_name);
            let _scope = span.enter();
            ctx.set_task_name(name.clone());
            match task(ctx) {
                Ok(v) => {
                    debug!("🟢 build task {}", name);
                    Ok(v)
                }
                Err(err) => {
                    error!("🔴 build task {}: {}", name, err);
                    Err(BuildError::TaskError { name, err })
                }
            }
        }));
    }

    fn push_dir(
        &mut self,
        options: &BuildOptions,
        tmp_dir: &Path,
        src: Option<&Path>,
        pre: Option<&Path>,
        relative: &Path,
    ) -> io::Result<()> {
        let mut map = BTreeMap::new();

        ordered_dir_list(&mut map, pre, true)?;
        ordered_dir_list(&mut map, src, false)?;

        for (filename, filestate) in map {
            let FileState { pre: pre_t, src: src_t } = filestate;
            let relative = relative.join(&filename);
            let path = CleanPath::new(relative.to_str().unwrap().to_string())
                .map_err(|_| err(&format!("weird characters in path {:?}", relative)))?;
            if pre_t.is_file() && !src_t.is_file() {
                let path = path.to_owned();
                self.push(&format!("rm {}", path), move |_| {
                    Ok(BuiltOperation::no_data(metadata::v1::Operation::Rm(metadata::v1::Rm {
                        path,
                        slice: None,
                    })))
                });
            }
            if src_t.is_dir() && !pre_t.is_dir() {
                let path = path.to_owned();
                self.push(&format!("mkdir {}", path), move |_| {
                    Ok(BuiltOperation::no_data(metadata::v1::Operation::MkDir { path }))
                });
            }
            if src_t.is_file() && !pre_t.is_file() {
                // add file
                let path = path.to_owned();
                let src_path = src.expect("src is_file").join(&filename);
                let tmp_path = tmp_dir.join(format!("task_{}", self.tasks.len()));
                let common = metadata::v1::Common {
                    path: path.clone(),
                    slice: None,
                    exe: src_t.is_exe(),
                    slice_handler: None,
                };
                for src_slice in slices(options, common, src_path, tmp_path)? {
                    self.push(
                        &format!("add {} [{} {}]", path, src_slice.offset, src_slice.size),
                        move |ctx| add_file(ctx, src_slice),
                    );
                }
            }
            if src_t.is_file() && pre_t.is_file() {
                // patch || check file
                let path = path.to_owned();
                let src_path = src.expect("src is_file").join(&filename);
                let pre_path = pre.expect("pre is_file").join(&filename);
                let tmp_path = tmp_dir.join(format!("task_{}", self.tasks.len()));
                let common = metadata::v1::Common {
                    path: path.clone(),
                    slice: None,
                    exe: src_t.is_exe(),
                    slice_handler: None,
                };
                let pre_slices = slices(options, common.clone(), pre_path, tmp_path.clone())?;
                for src_slice in slices(options, common, src_path, tmp_path)? {
                    let pre_slice = pre_slices
                        .iter()
                        .find(|pre_slice| pre_slice.common.slice == src_slice.common.slice);
                    match pre_slice {
                        Some(pre_slice) => {
                            let pre_slice = pre_slice.clone();
                            self.push(
                                &format!(
                                    "patch {} [{} {}] -> [{} {}]",
                                    path,
                                    pre_slice.offset,
                                    pre_slice.size,
                                    src_slice.offset,
                                    src_slice.size
                                ),
                                move |ctx| patch_file(ctx, src_slice, pre_slice),
                            );
                        }
                        None => {
                            self.push(
                                &format!("add {} [{} {}]", path, src_slice.offset, src_slice.size),
                                move |ctx| add_file(ctx, src_slice),
                            );
                        }
                    }
                }
            }

            if src_t.is_dir() || pre_t.is_dir() {
                let src = if src_t.is_dir() {
                    Some(src.expect("src is_dir").join(&filename))
                } else {
                    None
                };
                let pre = if pre_t.is_dir() {
                    Some(pre.expect("pre is_dir").join(&filename))
                } else {
                    None
                };
                self.push_dir(
                    options,
                    tmp_dir,
                    match src {
                        Some(ref path) => Some(Path::new(path)),
                        None => None,
                    },
                    match pre {
                        Some(ref path) => Some(Path::new(path)),
                        None => None,
                    },
                    &relative,
                )?;
            }

            if pre_t.is_dir() && !src_t.is_dir() {
                let path = path.to_owned();
                self.push(&format!("rmdir {}", path), |_| {
                    Ok(BuiltOperation::no_data(metadata::v1::Operation::RmDir { path }))
                });
            }
        }

        Ok(())
    }
}

struct Encoded<'a> {
    encoder_options: &'a CoderOptions,
    path: PathBuf,
    data_size: u64,
    data_sha1: Sha1Hash,
    final_size: u64,
    final_sha1: Sha1Hash,
}

#[instrument(skip(ctx, encoders_options, mk_encoder))]
fn best_encoder<'a>(
    ctx: &mut BuildTaskCtx,
    encoders_options: &'a [CoderOptions],
    mk_encoder: impl Fn(&CoderOptions, fs::File) -> io::Result<CheckCoder<fs::File, io::CheckSha1Size>>,
    src_slice: &Slice,
) -> io::Result<Encoded<'a>> {
    let mut best: Option<Encoded<'a>> = None;
    for encoder_options in encoders_options {
        if src_slice.size > encoder_options.max_size()?
            || src_slice.size < encoder_options.min_size()?
        {
            continue;
        }

        let mut enc_path = src_slice.tmp_path.as_os_str().to_owned();
        enc_path.push(format!(".{}", encoder_options.name()));
        let mut src_file = src_slice.open()?;
        let enc_file = fs::File::create(&enc_path)?;
        let mut encoder = mk_encoder(encoder_options, enc_file)?;
        let mut buffer = [0u8; io::BUFFER_SIZE];
        loop {
            let read = src_file.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            ctx.inc(read as u64);
            encoder.write_all(&buffer[..read])?;
        }

        encoder.flush()?;
        let input_checks = encoder.input_checks();
        let final_size = input_checks.bytes;
        let final_sha1 = input_checks.sha1();
        let mut output_checks = encoder.finish()?.check;
        let data_size = output_checks.bytes;
        let data_sha1 = output_checks.sha1();

        let ratio = (data_size * 100) / final_size;

        let encoded = Encoded {
            path: PathBuf::from(&enc_path),
            encoder_options,
            data_size,
            data_sha1,
            final_size,
            final_sha1,
        };

        io::assert_eq(encoded.final_size, src_slice.size, "src file size")?;
        let enc_len = fs::metadata(&enc_path)?.len();
        io::assert_eq(encoded.data_size, enc_len, "data file size")?;

        if ratio < encoder_options.min_ratio()? {
            io::remove_file(&encoded.path)?;
            continue;
        }

        best = Some(match best {
            Some(best) if encoded.data_size >= best.data_size => {
                io::remove_file(&encoded.path)?;
                best
            }
            Some(best) => {
                io::remove_file(&best.path)?;
                encoded
            }
            None => encoded,
        });
    }

    let best = match best {
        None => return Err(io::Error::new(io::ErrorKind::InvalidInput, "no compressor")),
        Some(best) => best,
    };

    Ok(best)
}

#[derive(Debug, Clone)]
struct Slice {
    common: metadata::v1::Common,
    src_path: PathBuf,
    tmp_path: PathBuf,
    offset: u64,
    size: u64,
}

impl Slice {
    fn open(&self) -> io::Result<io::Slice<fs::File>> {
        Ok(io::Slice::new(fs::File::open(&self.src_path)?, self.offset, self.size)?)
    }
}

fn slices(
    _options: &BuildOptions,
    common: metadata::v1::Common,
    src_path: PathBuf,
    tmp_path: PathBuf,
) -> io::Result<Vec<Slice>> {
    #[cfg(feature = "ue4pak")]
    if common.path.ends_with(".pak") {
        return ue4pak_slices(common, src_path, tmp_path);
    }

    let size = fs::metadata(&src_path)?.len();
    let slice = Slice { common, src_path, tmp_path, offset: 0, size };
    Ok(vec![slice])
}

#[cfg(feature = "ue4pak")]
fn ue4pak_slices(
    mut common: metadata::v1::Common,
    src_path: PathBuf,
    tmp_path: PathBuf,
) -> io::Result<Vec<Slice>> {
    use ue4pak::PakIndex;

    const INDEX_UUID: &str = "45882943-211b-46ac-bc43-fc905708f349";
    const INFO_UUID: &str = "19bf7388-d022-42ec-8c16-effa9f04c301";

    common.slice_handler = Some(CleanName::from_static_str("sliced"));

    let mut src_file = fs::File::open(&src_path)?;
    let size = src_file.metadata()?.len();
    let pak_file = ue4pak::PakFile::load_any(&mut io::BufReader::new(&mut src_file))?;
    let pak_info = pak_file.info();

    let mut cuts = Vec::new();

    let new_cut = |path: &str, offset: u64| {
        let slice = CleanPath::new(Sha1Hash::digest(path.as_bytes()).to_string())
            .expect("sha1 is cleanpath valid");
        (offset, slice)
    };
    cuts.push(new_cut(INDEX_UUID, pak_info.index_offset));
    cuts.push(new_cut(INFO_UUID, pak_info.index_offset + pak_info.index_size));
    match pak_file.index() {
        PakIndex::V1(v1) => {
            for (path, entry) in v1.named_entries() {
                cuts.push(new_cut(path, entry.offset));
            }
        }
        PakIndex::V2(v2) => {
            for (hash, entry) in v2.hashed_entries() {
                let slice =
                    CleanPath::new(format!("{:x?}", hash)).expect("sha1 is cleanpath valid");
                cuts.push((entry.offset, slice));
            }
        }
    }
    cuts.sort_by_key(|&(offset, _)| offset);

    let mut slices = Vec::new();
    let mut it = cuts.into_iter();
    let mut prev = it.next().unwrap();
    for cut in it {
        let slice = Slice {
            common: metadata::v1::Common { slice: Some(prev.1), ..common.clone() },
            src_path: src_path.clone(),
            tmp_path: tmp_path.clone(),
            offset: prev.0,
            size: cut.0 - prev.0,
        };
        slices.push(slice);
        prev = cut;
    }
    let slice = Slice {
        common: metadata::v1::Common { slice: Some(prev.1), ..common.clone() },
        src_path,
        tmp_path,
        offset: prev.0,
        size: size - prev.0,
    };
    slices.push(slice);
    Ok(slices)
}

fn add_file(ctx: &mut BuildTaskCtx, src_slice: Slice) -> Result<BuiltOperation, io::Error> {
    let options = ctx.options.clone();
    ctx.set_len(src_slice.size * options.compressors.len() as u64);

    let best_compressor = best_encoder(
        ctx,
        &options.compressors,
        |encoder_options, enc_file| CheckCoder::encoder(encoder_options, enc_file),
        &src_slice,
    )?;
    let op = metadata::v1::Operation::Add(metadata::v1::Add {
        common: src_slice.common,
        data_offset: 0,
        data_size: best_compressor.data_size,
        data_sha1: best_compressor.data_sha1,
        data_compression: CleanName::new(best_compressor.encoder_options.name().to_string())
            .expect("supported encoder name to be clean"),
        final_offset: 0,
        final_size: best_compressor.final_size,
        final_sha1: best_compressor.final_sha1,
    });

    Ok(BuiltOperation::with_data(best_compressor.path, op))
}

fn patch_file(
    ctx: &mut BuildTaskCtx,
    src_slice: Slice,
    pre_slice: Slice,
) -> Result<BuiltOperation, io::Error> {
    let options = ctx.options.clone();
    let mut are_equals = src_slice.size == pre_slice.size;

    let mut pre_file = io::CheckReader::new(pre_slice.open()?);
    let mut pre_buffer = [0u8; BUFFER_SIZE];
    if are_equals {
        // same len, let's check if content is the same
        let mut src_file = src_slice.open()?;
        let mut src_buffer = [0u8; BUFFER_SIZE];
        while are_equals {
            let read = pre_file.read(&mut pre_buffer)?;
            if read == 0 {
                break;
            }
            src_file.read_exact(&mut src_buffer[..read])?;
            are_equals = &src_buffer[..read] == &pre_buffer[..read];
        }
        if are_equals {
            // same content
            return Ok(BuiltOperation::no_data(metadata::v1::Operation::Check(
                metadata::v1::Check {
                    common: src_slice.common,
                    local_offset: 0,
                    local_size: pre_file.read_bytes(),
                    local_sha1: pre_file.sha1(),
                },
            )));
        }
    }
    loop {
        let read = pre_file.read(&mut pre_buffer)?;
        if read == 0 {
            break;
        }
    }
    let pre_sha1 = pre_file.sha1();
    io::assert_eq(pre_file.read_bytes(), pre_slice.size, "pre file size")?;
    drop(pre_file);

    ctx.set_len(src_slice.size * options.compressors.len() as u64);

    let best_patcher = best_encoder(
        ctx,
        &options.patchers,
        |patcher_options, enc_file| {
            let pre_file = pre_slice.open()?;
            CheckCoder::patch_encoder(patcher_options, pre_file, enc_file)
        },
        &src_slice,
    )?;
    let best_compressor = best_encoder(
        ctx,
        &options.compressors,
        |encoder_options, enc_file| CheckCoder::encoder(encoder_options, enc_file),
        &Slice {
            common: metadata::v1::Common {
                path: CleanPath::from_static_str("unreachable"),
                slice: None,
                exe: false,
                slice_handler: None,
            },
            src_path: best_patcher.path.clone(),
            tmp_path: best_patcher.path.clone(),
            offset: 0,
            size: best_patcher.data_size,
        },
    )?;
    let op = if best_patcher.encoder_options.name() == "raw" {
        // i.e. patch is bigger than file
        metadata::v1::Operation::Add(metadata::v1::Add {
            common: src_slice.common,
            data_offset: 0,
            data_size: best_compressor.data_size,
            data_sha1: best_compressor.data_sha1,
            data_compression: CleanName::new(best_patcher.encoder_options.name().to_string())
                .expect("supported encoder name to be clean"),
            final_offset: 0,
            final_size: best_patcher.final_size,
            final_sha1: best_patcher.final_sha1,
        })
    } else {
        metadata::v1::Operation::Patch(metadata::v1::Patch {
            common: src_slice.common,
            data_offset: 0,
            data_size: best_compressor.data_size,
            data_sha1: best_compressor.data_sha1,
            data_compression: CleanName::new(best_compressor.encoder_options.name().to_string())
                .expect("supported encoder name to be clean"),
            patch_type: CleanName::new(best_patcher.encoder_options.name().to_string())
                .expect("supported encoder name to be clean"),
            local_offset: 0,
            local_size: pre_slice.size,
            local_sha1: pre_sha1,
            final_offset: 0,
            final_size: best_patcher.final_size,
            final_sha1: best_patcher.final_sha1,
        })
    };

    Ok(BuiltOperation::with_data(best_compressor.path, op))
}
