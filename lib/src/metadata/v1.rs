//! Version 1 metadata definition
use std::fmt;

use serde::{Deserialize, Serialize};

use super::{maybe_cleanname, u64_str, CleanName, CleanPath, Sha1Hash};
use crate::workspace::UpdatePosition;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Version {
    pub revision: CleanName,
    pub description: String,
}

impl super::Version for Version {
    fn revision(&self) -> &CleanName {
        &self.revision
    }

    fn description(&self) -> &str {
        &self.description
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Package {
    #[serde(with = "maybe_cleanname")]
    pub from: Option<CleanName>,
    pub to: CleanName,
    #[serde(with = "u64_str")]
    pub size: u64,
}

impl Package {
    fn package_name(&self, suffix: &'static str) -> CleanName {
        CleanName::new(match &self.from {
            Some(from) => format!("patch{}_{}{}", from, self.to, suffix),
            None => format!("complete_{}{}", self.to, suffix),
        })
        .expect("name to be clean")
    }
}

impl super::Package for Package {
    fn from(&self) -> Option<&CleanName> {
        self.from.as_ref()
    }
    fn to(&self) -> &CleanName {
        &self.to
    }
    fn size(&self) -> u64 {
        self.size
    }
    fn package_data_name(&self) -> CleanName {
        self.package_name("")
    }
    fn package_metadata_name(&self) -> CleanName {
        self.package_name(".metadata")
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Common {
    pub path: CleanPath,
    #[serde(rename = "slice")]
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub slice: Option<CleanPath>,
    #[serde(default)]
    pub exe: bool,
    /// Name of the slice handler that will be available for other
    /// operations with a slice and the same path
    #[serde(rename = "sliceHandler")]
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub slice_handler: Option<CleanName>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Add {
    #[serde(flatten)]
    pub common: Common,

    #[serde(rename = "dataOffset")]
    #[serde(with = "u64_str")]
    pub data_offset: u64,
    #[serde(rename = "dataSize")]
    #[serde(with = "u64_str")]
    pub data_size: u64,
    #[serde(rename = "dataSha1")]
    pub data_sha1: Sha1Hash,
    #[serde(rename = "dataCompression")]
    pub data_compression: CleanName,

    #[serde(rename = "finalOffset")]
    #[serde(default)]
    #[serde(skip_serializing_if = "u64_str::is_zero")]
    #[serde(with = "u64_str")]
    pub final_offset: u64,
    #[serde(rename = "finalSize")]
    #[serde(with = "u64_str")]
    pub final_size: u64,
    #[serde(rename = "finalSha1")]
    pub final_sha1: Sha1Hash,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Patch {
    #[serde(flatten)]
    pub common: Common,

    pub data_offset: u64,
    #[serde(rename = "dataSize")]
    #[serde(with = "u64_str")]
    pub data_size: u64,
    #[serde(rename = "dataSha1")]
    pub data_sha1: Sha1Hash,
    #[serde(rename = "dataCompression")]
    pub data_compression: CleanName,

    #[serde(rename = "patchType")]
    pub patch_type: CleanName,

    #[serde(rename = "localOffset")]
    #[serde(default)]
    #[serde(skip_serializing_if = "u64_str::is_zero")]
    #[serde(with = "u64_str")]
    pub local_offset: u64,
    #[serde(rename = "localSize")]
    #[serde(with = "u64_str")]
    pub local_size: u64,
    #[serde(rename = "localSha1")]
    pub local_sha1: Sha1Hash,

    #[serde(rename = "finalOffset")]
    #[serde(default)]
    #[serde(skip_serializing_if = "u64_str::is_zero")]
    #[serde(with = "u64_str")]
    pub final_offset: u64,
    #[serde(rename = "finalSize")]
    #[serde(with = "u64_str")]
    pub final_size: u64,
    #[serde(rename = "finalSha1")]
    pub final_sha1: Sha1Hash,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Check {
    #[serde(flatten)]
    pub common: Common,

    #[serde(rename = "localOffset")]
    #[serde(default)]
    #[serde(skip_serializing_if = "u64_str::is_zero")]
    #[serde(with = "u64_str")]
    pub local_offset: u64,
    #[serde(rename = "localSize")]
    #[serde(with = "u64_str")]
    pub local_size: u64,
    #[serde(rename = "localSha1")]
    pub local_sha1: Sha1Hash,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Rm {
    pub path: CleanPath,
    #[serde(rename = "slice")]
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub slice: Option<CleanPath>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
pub enum Operation {
    #[serde(rename = "add")]
    Add(Add),
    #[serde(rename = "patch")]
    Patch(Patch),
    #[serde(rename = "check")]
    Check(Check),
    #[serde(rename = "rm")]
    Rm(Rm),
    #[serde(rename = "mkdir")]
    MkDir { path: CleanPath },
    #[serde(rename = "rmdir")]
    RmDir { path: CleanPath },
}

impl Operation {
    pub fn as_check_operation(&self) -> Option<Operation> {
        match self {
            Operation::Add(Add { common, final_offset, final_size, final_sha1, .. })
            | Operation::Patch(Patch { common, final_offset, final_size, final_sha1, .. }) => {
                Some(Operation::Check(Check {
                    common: common.clone(),
                    local_offset: *final_offset,
                    local_size: *final_size,
                    local_sha1: final_sha1.clone(),
                }))
            }
            Operation::Check { .. } | Operation::MkDir { .. } => Some(self.clone()),
            Operation::RmDir { .. } | Operation::Rm { .. } => None,
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub enum State {
    New,
    Stable { version: CleanName },
    Corrupted { version: CleanName, failures: Vec<Failure> },
    Updating(StateUpdating),
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(untagged)]
pub enum Failure {
    Path { path: CleanPath },
    Slice { path: CleanPath, slice: CleanPath },
}

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Failure::Path { path } => write!(f, "{}", path),
            Failure::Slice { path, slice } => write!(f, "{}#{}", path, slice),
        }
    }
}

impl Failure {
    pub fn path(&self) -> &CleanPath {
        match self {
            Failure::Path { path } | Failure::Slice { path, .. } => path,
        }
    }

    pub fn slice(&self) -> Option<&CleanPath> {
        match self {
            Failure::Path { .. } => None,
            Failure::Slice { slice, .. } => Some(slice),
        }
    }
}

impl PartialOrd for Failure {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Failure {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.path(), self.slice()).cmp(&(other.path(), other.slice()))
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct StateUpdating {
    #[serde(with = "crate::metadata::maybe_cleanname")]
    pub from: Option<CleanName>,
    pub to: CleanName,
    pub(crate) available: UpdatePosition,
    pub(crate) applied: UpdatePosition,
    #[serde(default)]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub failures: Vec<Failure>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub previous_failures: Vec<Failure>,
    #[serde(skip)]
    pub check_only: bool,
}

impl StateUpdating {
    pub fn new(from: Option<CleanName>, to: CleanName, failures: Vec<Failure>) -> StateUpdating {
        StateUpdating {
            from,
            to,
            available: UpdatePosition::default(),
            applied: UpdatePosition::default(),
            failures,
            previous_failures: Vec::default(),
            check_only: false,
        }
    }

    pub(crate) fn update_with(&mut self, other: Self) {
        self.from = other.from;
        self.to = other.to;
        self.available = other.available;
        self.applied = other.applied;
        self.check_only = other.check_only;
        if !other.failures.is_empty() || !other.previous_failures.is_empty() {
            self.failures.extend(other.failures);
            self.failures.extend(other.previous_failures);
            self.failures.sort();
            self.failures.dedup();
        }
    }

    pub(crate) fn clear_progress(&mut self) {
        self.available = UpdatePosition::default();
        self.applied = UpdatePosition::default();
    }

    pub(crate) fn dedup_failures(&mut self) {
        self.failures.extend(std::mem::take(&mut self.previous_failures));
        self.failures.sort();
        self.failures.dedup();
    }
}
