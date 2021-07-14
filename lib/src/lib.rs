mod codecs;
mod handlers;
pub mod histogram;
mod io;
pub mod link;
pub mod metadata;
pub mod repository;
mod sync;
pub mod workspace;

pub use link::AutoRepository;
pub use repository::Repository;
pub use workspace::Workspace;

#[cfg(test)]
pub mod tests {
    use std::{
        collections::BTreeSet,
        fmt, fs,
        path::{Path, PathBuf},
    };

    use tracing::log;

    pub fn init() {
        let _ =
            env_logger::builder().filter_level(log::LevelFilter::Debug).is_test(true).try_init();
    }

    #[derive(Eq, PartialEq)]
    pub struct Bytes<'a>(pub &'a [u8]);

    impl<'a> fmt::Debug for Bytes<'a> {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            fmt::Display::fmt(self, f)
        }
    }

    impl<'a> fmt::Display for Bytes<'a> {
        fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
            write!(fmt, "b\"")?;
            for &byte in self.0.iter() {
                if byte == b'\\' {
                    write!(fmt, r"\\")?;
                } else if !byte.is_ascii_control() {
                    write!(fmt, "{}", byte as char)?;
                } else {
                    write!(fmt, "\\x{:02x}", byte)?;
                }
            }
            write!(fmt, "\"")
        }
    }

    #[track_caller]
    pub fn assert_fs_eq(path0: &Path, path1: &Path) {
        let m0 = fs::metadata(path0).unwrap();
        let m1 = fs::metadata(path1).unwrap();
        if m0.is_file() && m1.is_file() {
            assert_eq!(
                Bytes(&fs::read(path0).unwrap()),
                Bytes(&fs::read(path1).unwrap()),
                "{:?} and {:?} content differ",
                path0,
                path1
            );
        } else if m0.is_dir() && m1.is_dir() {
            let dir0 = fs::read_dir(path0)
                .unwrap()
                .map(|res| res.map(|e| e.file_name()))
                .collect::<Result<BTreeSet<_>, _>>()
                .unwrap();
            let dir1 = fs::read_dir(path1)
                .unwrap()
                .map(|res| res.map(|e| e.file_name()))
                .collect::<Result<BTreeSet<_>, _>>()
                .unwrap();
            for e in dir0.difference(&dir1) {
                panic!("{:?} is not present in {:?}", e, path1);
            }
            for e in dir1.difference(&dir0) {
                panic!("{:?} is not present in {:?}", e, path0);
            }
            for (filename0, filename1) in dir0.iter().zip(dir1.iter()) {
                assert_fs_eq(&path0.join(filename0), &path1.join(filename1));
            }
        } else {
            panic!(
                "{:?} ({:?}) and {:?} ({:?}) aren't the same file types",
                path0,
                m0.file_type(),
                path1,
                m1.file_type()
            );
        }
    }

    pub fn tmp_clone_dir(data_name: &str, tmp_name: &str) -> PathBuf {
        let tmp_path = PathBuf::from("target/tests").join(tmp_name);
        let _ = fs::remove_dir_all(&tmp_path);
        fs::create_dir_all(&tmp_path).unwrap();

        let data_path = data(data_name);

        let mut options = fs_extra::dir::CopyOptions::new();
        options.content_only = true;
        fs_extra::dir::copy(&data_path, &tmp_path, &options).unwrap();

        tmp_path
    }

    pub fn tmp_dir(name: &str) -> PathBuf {
        let path = PathBuf::from("target/tests").join(name);
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).unwrap();
        path
    }

    pub fn data(name: &str) -> PathBuf {
        let path = PathBuf::from("tests/data").join(name);
        path
    }
}
