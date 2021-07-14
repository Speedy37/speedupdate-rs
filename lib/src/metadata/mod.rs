//! Workspace and Repository metadata definition, serde, ...
mod dijkstra;
pub mod v1;

use std::collections::HashMap;
use std::fmt;
use std::ops::{Deref, Range};
use std::path::Path;
use std::slice;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};

/// Common version information
pub trait Version {
    fn revision(&self) -> &CleanName;
    fn description(&self) -> &str;
}

/// Common package information
pub trait Package {
    /// True if this package doesn't require a previous package to be installed
    ///
    /// i.e. `from().is_none()`
    fn is_standalone(&self) -> bool {
        self.from().is_none()
    }
    fn from(&self) -> Option<&CleanName>;
    fn to(&self) -> &CleanName;
    fn size(&self) -> u64;
    fn package_data_name(&self) -> CleanName;
    fn package_metadata_name(&self) -> CleanName;
}

/// Operation type (add, patch, check, ...)
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum OperationKind {
    Add,
    Patch,
    Check,
    Rm,
    MkDir,
    RmDir,
}

/// Common operation info
pub trait Operation {
    fn kind(&self) -> OperationKind;
    fn path(&self) -> &CleanPath;
    fn slice(&self) -> Option<&CleanPath>;
    fn slice_handler(&self) -> Option<&CleanName>;

    fn range(&self) -> Option<Range<u64>>;
    fn check_size(&self) -> u64;
    fn data_size(&self) -> u64;
    fn final_size(&self) -> u64;

    fn set_data_offset(&mut self, offset: u64);
}

pub(crate) mod maybe_cleanname {
    use serde::{self, Deserialize, Deserializer, Serializer};

    use super::CleanName;

    pub fn serialize<S>(value: &Option<CleanName>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = match value {
            Some(value) => &*value,
            None => "",
        };
        serializer.serialize_str(s)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<CleanName>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let path = String::deserialize(deserializer)?;
        if path.len() == 0 {
            Ok(None)
        } else {
            CleanName::new(path).map(Some).map_err(|path| {
                serde::de::Error::invalid_value(
                    serde::de::Unexpected::Str(&path),
                    &"a clean name (i.e. [A-Za-Z0-9_.-]+)",
                )
            })
        }
    }
}

pub(crate) mod u64_str {
    use serde::{self, Deserialize, Deserializer, Serializer};

    pub fn is_zero(value: &u64) -> bool {
        *value == 0
    }

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
        let s = String::deserialize(deserializer)?;
        u64::from_str_radix(&*s, 10).map_err(|err| {
            serde::de::Error::invalid_value(
                serde::de::Unexpected::Str(&s),
                &err.to_string().as_str(),
            )
        })
    }
}

/// A sha1 hash
#[derive(Clone, Eq, PartialEq)]
pub struct Sha1Hash {
    hash: [u8; 20],
}

impl Sha1Hash {
    pub fn new(hash: [u8; 20]) -> Self {
        Self { hash }
    }

    pub fn digest(buf: &[u8]) -> Self {
        Self::new(Sha1::digest(buf).into())
    }
}

impl From<[u8; 20]> for Sha1Hash {
    fn from(hash: [u8; 20]) -> Self {
        Self { hash }
    }
}

impl Deref for Sha1Hash {
    type Target = [u8; 20];

    fn deref(&self) -> &[u8; 20] {
        &self.hash
    }
}

impl FromStr for Sha1Hash {
    type Err = &'static str;

    fn from_str(hex: &str) -> Result<Self, Self::Err> {
        fn val(c: u8) -> Result<u8, &'static str> {
            match c {
                b'A'..=b'F' => Ok(c - b'A' + 10),
                b'a'..=b'f' => Ok(c - b'a' + 10),
                b'0'..=b'9' => Ok(c - b'0'),
                _ => Err("invalid hex char"),
            }
        }

        let hex = hex.as_bytes();
        if hex.len() != 20 * 2 {
            return Err("invalid string length");
        }

        let mut hash = [0u8; 20];
        for (i, byte) in hash.iter_mut().enumerate() {
            *byte = val(hex[2 * i])? << 4 | val(hex[2 * i + 1])?;
        }
        Ok(Self { hash })
    }
}

impl fmt::Debug for Sha1Hash {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl fmt::Display for Sha1Hash {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for i in self.hash.iter() {
            write!(f, "{:02x}", i)?;
        }
        Ok(())
    }
}

impl serde::Serialize for Sha1Hash {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.to_string().serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for Sha1Hash {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let hex = String::deserialize(deserializer)?;
        Self::from_str(&hex)
            .map_err(|err| serde::de::Error::invalid_value(serde::de::Unexpected::Str(&hex), &err))
    }
}

/// A clean relative path (no '..' or '.' component, '/' separator only)
#[derive(Debug, Clone, Serialize, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[serde(transparent)]
pub struct CleanPath {
    path: String,
}

impl CleanPath {
    pub fn as_str(&self) -> &str {
        &self.path
    }
}

impl std::fmt::Display for CleanPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.path, f)
    }
}

impl AsRef<Path> for CleanPath {
    fn as_ref(&self) -> &Path {
        self.path.as_ref()
    }
}

impl Deref for CleanPath {
    type Target = str;

    fn deref(&self) -> &str {
        &self.path
    }
}

impl CleanPath {
    pub fn from_static_str(path: &'static str) -> Self {
        Self::new(path.to_string()).expect("static str to match clean path requirements")
    }

    pub fn new(mut path: String) -> Result<Self, String> {
        if path.contains('\\') {
            path = path.replace('\\', "/");
        }
        let is_clean = path.split('/').all(|component| component != "." && component != "..");
        if is_clean && path.len() > 0 {
            Ok(Self { path })
        } else {
            Err(path)
        }
    }
}

impl<'de> serde::Deserialize<'de> for CleanPath {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let path = String::deserialize(deserializer)?;

        Self::new(path).map_err(|path| {
            serde::de::Error::invalid_value(
                serde::de::Unexpected::Str(&path),
                &"a clean path (no '..')",
            )
        })
    }
}

/// A clean name (i.e  `[A-Za-Z0-9_.-]+`)
#[derive(Debug, Clone, Serialize, Eq, PartialEq, Hash)]
#[serde(transparent)]
pub struct CleanName {
    name: String,
}

impl CleanName {
    pub fn as_str(&self) -> &str {
        &self.name
    }
}

impl Deref for CleanName {
    type Target = str;

    fn deref(&self) -> &str {
        &self.name
    }
}

impl AsRef<Path> for CleanName {
    fn as_ref(&self) -> &Path {
        self.name.as_ref()
    }
}

impl std::fmt::Display for CleanName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.name, f)
    }
}

impl CleanName {
    pub fn from_static_str(path: &'static str) -> Self {
        Self::new(path.to_string()).expect("static str to match clean name requirements")
    }
    pub fn new(path: String) -> Result<Self, String> {
        if path.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-' || b == b'.')
            && path.len() > 0
        {
            Ok(Self { name: path })
        } else {
            Err(path)
        }
    }
}

impl<'de> serde::Deserialize<'de> for CleanName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let path = String::deserialize(deserializer)?;

        Self::new(path).map_err(|path| {
            serde::de::Error::invalid_value(
                serde::de::Unexpected::Str(&path),
                &"a clean name (i.e. [A-Za-Z0-9_.-]+)",
            )
        })
    }
}

/// Repository `current` JSON file definition
#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "version")]
pub enum Current {
    #[serde(rename = "1")]
    V1 { current: v1::Version },
}

impl Current {
    pub fn filename() -> &'static str {
        "current"
    }

    pub fn version(&self) -> &CleanName {
        match self {
            &Current::V1 { ref current } => &current.revision,
        }
    }
}

/// Repository `versions` JSON file definition
#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "version")]
pub enum Versions {
    #[serde(rename = "1")]
    V1 { versions: Vec<v1::Version> },
}

impl Versions {
    pub fn filename() -> &'static str {
        "versions"
    }

    pub fn iter(&self) -> impl Iterator<Item = &dyn Version> {
        match self {
            &Versions::V1 { ref versions } => versions.iter().map(|v| {
                let v: &dyn Version = v;
                v
            }),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            &Versions::V1 { ref versions } => versions.len(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "version")]
pub enum Packages {
    #[serde(rename = "1")]
    V1 { packages: Vec<v1::Package> },
}

impl Packages {
    pub fn filename() -> &'static str {
        "packages"
    }

    pub fn iter(&self) -> impl Iterator<Item = &dyn Package> {
        match self {
            &Packages::V1 { ref packages } => packages.iter().map(|p| {
                let p: &dyn Package = p;
                p
            }),
        }
    }

    pub(crate) fn as_slice(&self) -> &[v1::Package] {
        match self {
            &Packages::V1 { ref packages } => &packages,
        }
    }

    pub fn len(&self) -> usize {
        match self {
            &Packages::V1 { ref packages } => packages.len(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "version")]
pub enum PackageMetadata {
    #[serde(rename = "1")]
    V1 { package: v1::Package, operations: Vec<v1::Operation> },
}

impl Package for PackageMetadata {
    fn from(&self) -> Option<&CleanName> {
        match self {
            &PackageMetadata::V1 { ref package, .. } => package.from(),
        }
    }
    fn to(&self) -> &CleanName {
        match self {
            &PackageMetadata::V1 { ref package, .. } => package.to(),
        }
    }
    fn size(&self) -> u64 {
        match self {
            &PackageMetadata::V1 { ref package, .. } => package.size(),
        }
    }
    fn package_data_name(&self) -> CleanName {
        match self {
            &PackageMetadata::V1 { ref package, .. } => package.package_data_name(),
        }
    }
    fn package_metadata_name(&self) -> CleanName {
        match self {
            &PackageMetadata::V1 { ref package, .. } => package.package_metadata_name(),
        }
    }
}

impl PackageMetadata {
    pub(crate) fn iter(&self) -> slice::Iter<v1::Operation> {
        match self {
            &PackageMetadata::V1 { ref operations, .. } => operations.iter(),
        }
    }
}

/// Find the shortest path accross packages
///
/// Returns [`Some(Vec<P>)`] if a path between `start` and `goal` exists
/// Otherwise returns [`None`]
pub fn shortest_path<'a: 'b, 'b, P>(
    start: Option<&'b CleanName>,
    goal: &'b CleanName,
    packages: &'a [P],
) -> Option<Vec<&'a P>>
where
    P: Package,
{
    let mut nodes: Vec<Vec<dijkstra::Edge>> = Vec::new();
    let mut name_to_idx: HashMap<Option<&'b CleanName>, usize> = HashMap::new();
    let mut idx_to_name: Vec<Option<&'b CleanName>> = Vec::new();
    let mut get_node_idx = |nodes: &mut Vec<Vec<dijkstra::Edge>>,
                            idx_to_name: &mut Vec<Option<&'b CleanName>>,
                            name: Option<&'b CleanName>|
     -> usize {
        let idx = name_to_idx.entry(name).or_insert_with(|| {
            let idx = nodes.len();
            nodes.push(Vec::new());
            idx_to_name.push(name);
            idx
        });
        *idx
    };
    let empty_idx = get_node_idx(&mut nodes, &mut idx_to_name, None);
    let start_idx = get_node_idx(&mut nodes, &mut idx_to_name, start);
    let goal_idx = get_node_idx(&mut nodes, &mut idx_to_name, Some(goal));
    if empty_idx != start_idx {
        nodes[start_idx].push(dijkstra::Edge { node: empty_idx, cost: 0 });
    }
    for package in packages {
        let from = get_node_idx(&mut nodes, &mut idx_to_name, package.from());
        let to = get_node_idx(&mut nodes, &mut idx_to_name, Some(package.to()));
        nodes[from].push(dijkstra::Edge { node: to, cost: package.size() });
    }

    let path = dijkstra::shortest_path(&nodes, start_idx, goal_idx);
    match path {
        Some(path) => {
            let mut path = path.as_slice();
            let mut ret = Vec::new();
            let mut from = start;
            if empty_idx != start_idx && path[0] == empty_idx {
                from = None;
                path = &path[1..];
            }
            for &p in path {
                let to = idx_to_name[p];
                let p = packages
                    .iter()
                    .find(|&package| package.from() == from && Some(package.to()) == to);
                if let Some(p) = p {
                    ret.push(p);
                }
                from = to;
            }
            assert!(path.len() > 0);
            Some(ret)
        }
        None => None,
    }
}

impl Operation for v1::Operation {
    fn kind(&self) -> OperationKind {
        match self {
            v1::Operation::Add(_) => OperationKind::Add,
            v1::Operation::Patch(_) => OperationKind::Patch,
            v1::Operation::Check(_) => OperationKind::Check,
            v1::Operation::Rm(_) => OperationKind::Rm,
            v1::Operation::MkDir { .. } => OperationKind::MkDir,
            v1::Operation::RmDir { .. } => OperationKind::RmDir,
        }
    }
    fn check_size(&self) -> u64 {
        match self {
            &v1::Operation::Check(v1::Check { local_size, .. }) => local_size,
            _ => 0,
        }
    }
    fn data_size(&self) -> u64 {
        match self {
            &v1::Operation::Add(v1::Add { data_size, .. }) => data_size,
            &v1::Operation::Patch(v1::Patch { data_size, .. }) => data_size,
            _ => 0,
        }
    }
    fn final_size(&self) -> u64 {
        match self {
            &v1::Operation::Add(v1::Add { final_size, .. }) => final_size,
            &v1::Operation::Patch(v1::Patch { final_size, .. }) => final_size,
            _ => 0,
        }
    }
    fn range(&self) -> Option<Range<u64>> {
        match self {
            &v1::Operation::Add(v1::Add { data_offset, data_size, .. })
            | &v1::Operation::Patch(v1::Patch { data_offset, data_size, .. }) => {
                Some(Range { start: data_offset, end: data_offset + data_size })
            }
            _ => None,
        }
    }

    fn set_data_offset(&mut self, offset: u64) {
        match self {
            v1::Operation::Add(v1::Add { data_offset, .. })
            | v1::Operation::Patch(v1::Patch { data_offset, .. }) => *data_offset = offset,
            _ => {}
        }
    }

    fn path(&self) -> &CleanPath {
        match self {
            v1::Operation::Add(v1::Add { common, .. })
            | v1::Operation::Patch(v1::Patch { common, .. })
            | v1::Operation::Check(v1::Check { common, .. }) => &common.path,
            v1::Operation::MkDir { path, .. }
            | v1::Operation::RmDir { path, .. }
            | v1::Operation::Rm(v1::Rm { path, .. }) => &path,
        }
    }

    fn slice(&self) -> Option<&CleanPath> {
        match self {
            v1::Operation::Add(v1::Add { common, .. })
            | v1::Operation::Patch(v1::Patch { common, .. })
            | v1::Operation::Check(v1::Check { common, .. }) => common.slice.as_ref(),
            v1::Operation::Rm(v1::Rm { slice, .. }) => slice.as_ref(),
            v1::Operation::MkDir { .. } | v1::Operation::RmDir { .. } => None,
        }
    }

    fn slice_handler(&self) -> Option<&CleanName> {
        match self {
            v1::Operation::Add(v1::Add { common, .. })
            | v1::Operation::Patch(v1::Patch { common, .. })
            | v1::Operation::Check(v1::Check { common, .. }) => common.slice_handler.as_ref(),
            v1::Operation::Rm(_) | v1::Operation::MkDir { .. } | v1::Operation::RmDir { .. } => {
                None
            }
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "version")]
pub enum WorkspaceState {
    #[serde(rename = "1")]
    V1 { state: v1::State },
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "version")]
pub enum WorkspaceChecks {
    #[serde(rename = "1")]
    V1 { operations: Vec<v1::Operation> },
}

impl WorkspaceChecks {
    pub(crate) fn iter(&self) -> slice::Iter<v1::Operation> {
        match self {
            &WorkspaceChecks::V1 { ref operations, .. } => operations.iter(),
        }
    }
}
