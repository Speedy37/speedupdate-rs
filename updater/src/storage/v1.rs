use crate::operation::{self, check_permission, FinalWriter};
use crate::storage;
use crate::updater::UpdateOptions;
use crate::workspace::WorkspaceFileManager;
use crate::BUFFER_SIZE;
use brotli::DecompressorWriter;
use std::fs;
use std::fs::File;
use std::io;
use std::io::{Read, Seek, Write};
use std::ops::Range;
use vcdiff_rs::{DecoderState, VCDiffDecoder};

mod u64_str {
  use serde::{self, Deserialize, Deserializer, Serializer};
  pub fn serialize<S>(value: &u64, serializer: S) -> Result<S::Ok, S::Error>
  where
    S: Serializer,
  {
    serializer.serialize_str(&value.to_string())
  }

  pub fn deserialize<'de, D>(deserializer: D) -> Result<u64, D::Error>
  where
    D: Deserializer<'de>,
  {
    let s = <&str>::deserialize(deserializer)?;
    u64::from_str_radix(s, 10).map_err(serde::de::Error::custom)
  }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Version {
  pub revision: String,
  pub description: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Package {
  pub from: String,
  pub to: String,
  #[serde(with = "u64_str")]
  pub size: u64,
}

impl Package {
  fn package_name(&self, suffix: &str) -> String {
    if self.from.is_empty() {
      format!("complete_{}{}", self.to, suffix)
    } else {
      format!("patch{}_{}{}", self.from, self.to, suffix)
    }
  }
}

impl storage::Package for Package {
  fn is_standalone(&self) -> bool {
    self.from.is_empty()
  }
  fn from(&self) -> &str {
    &self.from
  }
  fn to(&self) -> &str {
    &self.to
  }
  fn size(&self) -> u64 {
    self.size
  }
  fn package_data_name(&self) -> String {
    self.package_name("")
  }
  fn package_metadata_name(&self) -> String {
    self.package_name(".metadata")
  }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
pub enum Operation {
  #[serde(rename = "add")]
  Add {
    path: String,
    #[serde(rename = "dataOffset")]
    #[serde(with = "u64_str")]
    data_offset: u64,
    #[serde(rename = "dataSize")]
    #[serde(with = "u64_str")]
    data_size: u64,
    #[serde(rename = "dataSha1")]
    data_sha1: String,
    #[serde(rename = "dataCompression")]
    data_compression: String,
    #[serde(rename = "finalSize")]
    #[serde(with = "u64_str")]
    final_size: u64,
    #[serde(rename = "finalSha1")]
    final_sha1: String,
    #[serde(default)]
    exe: bool,
  },
  #[serde(rename = "patch")]
  Patch {
    path: String,
    #[serde(rename = "dataOffset")]
    #[serde(with = "u64_str")]
    data_offset: u64,
    #[serde(rename = "dataSize")]
    #[serde(with = "u64_str")]
    data_size: u64,
    #[serde(rename = "dataSha1")]
    data_sha1: String,
    #[serde(rename = "dataCompression")]
    data_compression: String,
    #[serde(rename = "patchType")]
    patch_type: String,
    #[serde(rename = "localSize")]
    #[serde(with = "u64_str")]
    local_size: u64,
    #[serde(rename = "localSha1")]
    local_sha1: String,
    #[serde(rename = "finalSize")]
    #[serde(with = "u64_str")]
    final_size: u64,
    #[serde(rename = "finalSha1")]
    final_sha1: String,
    #[serde(default)]
    exe: bool,
  },
  #[serde(rename = "check")]
  Check {
    path: String,
    #[serde(rename = "localSize")]
    #[serde(with = "u64_str")]
    local_size: u64,
    #[serde(rename = "localSha1")]
    local_sha1: String,
    #[serde(default)]
    exe: bool,
  },
  #[serde(rename = "mkdir")]
  MkDir { path: String },
  #[serde(rename = "rmdir")]
  RmDir { path: String },
  #[serde(rename = "rm")]
  Rm { path: String },
}

impl Operation {
  pub fn as_check_operation(&self) -> Option<Operation> {
    match self {
      &Operation::Add {
        ref path,
        final_size,
        ref final_sha1,
        exe,
        ..
      }
      | &Operation::Patch {
        ref path,
        final_size,
        ref final_sha1,
        exe,
        ..
      } => Some(Operation::Check {
        path: path.clone(),
        local_size: final_size,
        local_sha1: final_sha1.clone(),
        exe,
      }),
      &Operation::Check { .. } | &Operation::MkDir { .. } => Some(self.clone()),
      &Operation::RmDir { .. } | &Operation::Rm { .. } => None,
    }
  }
}

impl operation::Operation for Operation {
  fn data_size(&self) -> u64 {
    match self {
      &Operation::Add { data_size, .. } => data_size,
      &Operation::Patch { data_size, .. } => data_size,
      _ => 0,
    }
  }
  fn final_size(&self) -> u64 {
    match self {
      &Operation::Add { final_size, .. } => final_size,
      &Operation::Patch { final_size, .. } => final_size,
      _ => 0,
    }
  }
  fn range(&self) -> Option<Range<u64>> {
    match self {
      &Operation::Add {
        data_offset,
        data_size,
        ..
      }
      | &Operation::Patch {
        data_offset,
        data_size,
        ..
      } => Some(Range {
        start: data_offset,
        end: data_offset + data_size,
      }),
      _ => None,
    }
  }

  fn path(&self) -> &str {
    match self {
      &Operation::Add { ref path, .. } => &path,
      &Operation::Patch { ref path, .. } => &path,
      &Operation::Check { ref path, .. } => &path,
      &Operation::MkDir { ref path, .. } => &path,
      &Operation::RmDir { ref path, .. } => &path,
      &Operation::Rm { ref path, .. } => &path,
    }
  }
  fn begin_apply(
    &self,
    file_manager: &WorkspaceFileManager,
    index: usize,
    update_options: &UpdateOptions,
  ) -> Result<Option<operation::ApplyGuard>, io::Error> {
    match self {
      &Operation::Add {
        ref path,
        data_size,
        ref data_sha1,
        ref data_compression,
        final_size,
        ref final_sha1,
        exe,
        ..
      } => {
        let tmp_path = file_manager.tmp_operation_path(index);
        let final_path = file_manager.dir().join(path);
        let tmp_file = fs::OpenOptions::new()
          .write(true)
          .create(true)
          .open(&tmp_path)?;
        check_permission(&tmp_file, exe)?;
        let writer = FinalWriter::new(tmp_file);
        Ok(Some(operation::ApplyGuard::new(
          data_size,
          decode_sha1_digest(data_sha1)?,
          final_size,
          decode_sha1_digest(final_sha1)?,
          final_path,
          writer.stats(),
          tmp_path,
          decompressor(data_compression, writer)?,
        )))
      }
      &Operation::Patch {
        ref path,
        data_size,
        ref data_sha1,
        ref data_compression,
        final_size,
        ref final_sha1,
        ref patch_type,
        local_size,
        exe,
        ..
      } => {
        let final_path = file_manager.dir().join(path);
        let current_local_size = fs::metadata(&final_path).map(|m| m.len())?;
        if current_local_size != local_size {
          return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
              "current_local_size {} != operation.local_size {}",
              current_local_size, local_size
            ),
          ));
        }
        let local_file = fs::OpenOptions::new()
          .read(true)
          .write(true)
          .open(&final_path)?;
        let tmp_path = file_manager.tmp_operation_path(index);
        let tmp_file = fs::OpenOptions::new()
          .write(true)
          .read(true)
          .create(true)
          .open(&tmp_path)?;
        check_permission(&tmp_file, exe)?;
        let writer = FinalWriter::new(tmp_file);
        Ok(Some(operation::ApplyGuard::new(
          data_size,
          decode_sha1_digest(data_sha1)?,
          final_size,
          decode_sha1_digest(final_sha1)?,
          final_path,
          writer.stats(),
          tmp_path,
          patch_applier(data_compression, patch_type, local_file, writer)?,
        )))
      }
      &Operation::Check {
        ref path,
        local_size,
        ref local_sha1,
        exe,
      } => {
        if update_options.check {
          operation::check_file(
            &file_manager.dir().join(path),
            local_size,
            decode_sha1_digest(local_sha1)?,
            exe,
          )
          .map(|_| None)
        } else {
          Ok(None)
        }
      }
      &Operation::MkDir { ref path, .. } => fs::create_dir_all(file_manager.dir().join(path))
        .map(|_| None)
        .or_else(|err| match err.kind() {
          io::ErrorKind::AlreadyExists => Ok(None),
          _ => Err(err),
        }),
      &Operation::RmDir { ref path, .. } => fs::remove_dir(file_manager.dir().join(path))
        .map(|_| None)
        .or_else(|err| match err.kind() {
          io::ErrorKind::NotFound => Ok(None),
          _ => Err(err),
        }),
      &Operation::Rm { ref path, .. } => fs::remove_file(file_manager.dir().join(path))
        .map(|_| None)
        .or_else(|err| match err.kind() {
          io::ErrorKind::NotFound => Ok(None),
          _ => Err(err),
        }),
    }
  }
}

fn val(c: u8) -> Result<u8, io::Error> {
  match c {
    b'A'...b'F' => Ok(c - b'A' + 10),
    b'a'...b'f' => Ok(c - b'a' + 10),
    b'0'...b'9' => Ok(c - b'0'),
    _ => Err(io::Error::new(io::ErrorKind::Other, "invalid hex char")),
  }
}

fn decode_sha1_digest<'a>(hex: &str) -> Result<[u8; 20], io::Error> {
  let hex = hex.as_bytes();
  if hex.len() / 2 != 20 {
    return Err(io::Error::new(
      io::ErrorKind::Other,
      "invalid string length",
    ));
  }

  let mut out = [0u8; 20];
  for (i, byte) in out.iter_mut().enumerate() {
    *byte = val(hex[2 * i])? << 4 | val(hex[2 * i + 1])?;
  }
  Ok(out)
}

fn decompressor(
  decompressor_name: &str,
  tmp_file: FinalWriter<File>,
) -> Result<Box<io::Write>, io::Error> {
  if decompressor_name == "brotli" {
    Ok(Box::new(DecompressorWriter::new(tmp_file, BUFFER_SIZE)))
  } else {
    Err(io::Error::new(io::ErrorKind::Other, "not implemented!"))
  }
}

struct VCDiffDecoderWriter<ORIGINAL: Read + Seek, TARGET: Write + Read + Seek> {
  decoder: VCDiffDecoder<ORIGINAL, TARGET>,
  state: DecoderState,
}

impl<ORIGINAL: Read + Seek, TARGET: Write + Read + Seek> Write
  for VCDiffDecoderWriter<ORIGINAL, TARGET>
{
  fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
    self.state = self.decoder.decode(buf)?;
    Ok(buf.len())
  }

  fn flush(&mut self) -> io::Result<()> {
    Ok(())
  }
}

fn patch_applier(
  decompressor_name: &str,
  patcher_name: &str,
  local_file: File,
  tmp_file: FinalWriter<File>,
) -> Result<Box<io::Write>, io::Error> {
  if decompressor_name == "brotli" && patcher_name == "vcdiff" {
    let patcher = VCDiffDecoderWriter {
      decoder: VCDiffDecoder::new(local_file, tmp_file, BUFFER_SIZE),
      state: DecoderState::WantMoreInputOrDone,
    };
    let decompressor = DecompressorWriter::new(patcher, BUFFER_SIZE);
    Ok(Box::new(decompressor))
  } else {
    Err(io::Error::new(io::ErrorKind::Other, "not implemented!"))
  }
}
