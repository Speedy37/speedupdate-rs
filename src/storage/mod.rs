mod dijkstra;
pub mod v1;

use std::slice;
use std::collections::BTreeMap;

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "version")]
pub enum Current {
  #[serde(rename = "1")] V1 { current: v1::Version },
}

impl Current {
  pub fn version(&self) -> &str {
    match self {
      &Current::V1 { ref current } => &current.revision,
    }
  }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "version")]
pub enum Versions {
  #[serde(rename = "1")] V1 { versions: Vec<v1::Version> },
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "version")]
pub enum Packages {
  #[serde(rename = "1")] V1 { packages: Vec<v1::Package> },
}

impl Packages {
  pub fn as_slice(&self) -> &[v1::Package] {
    match self {
      &Packages::V1 { ref packages } => &packages,
    }
  }
}

pub trait Package {
  fn is_standalone(&self) -> bool;
  fn from(&self) -> &str;
  fn to(&self) -> &str;
  fn size(&self) -> u64;
  fn package_data_name(&self) -> String;
  fn package_metadata_name(&self) -> String;
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "version")]
pub enum PackageMetadata {
  #[serde(rename = "1")] V1 {
    package: v1::Package,
    operations: Vec<v1::Operation>,
  },
}

impl Package for PackageMetadata {
  fn is_standalone(&self) -> bool {
    match self {
      &PackageMetadata::V1 { ref package, .. } => package.is_standalone(),
    }
  }
  fn from(&self) -> &str {
    match self {
      &PackageMetadata::V1 { ref package, .. } => package.from(),
    }
  }
  fn to(&self) -> &str {
    match self {
      &PackageMetadata::V1 { ref package, .. } => package.to(),
    }
  }
  fn size(&self) -> u64 {
    match self {
      &PackageMetadata::V1 { ref package, .. } => package.size(),
    }
  }
  fn package_data_name(&self) -> String {
    match self {
      &PackageMetadata::V1 { ref package, .. } => package.package_data_name(),
    }
  }
  fn package_metadata_name(&self) -> String {
    match self {
      &PackageMetadata::V1 { ref package, .. } => package.package_metadata_name(),
    }
  }
}

impl PackageMetadata {
  pub fn iter(&self) -> slice::Iter<v1::Operation> {
    match self {
      &PackageMetadata::V1 { ref operations, .. } => operations.iter(),
    }
  }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "version")]
pub enum LocalRepository {
  #[serde(rename = "1")]
  V1 {
    #[serde(rename = "Revision")] revision: String,
    #[serde(rename = "UpdateInProgress")] update_in_progress: bool,
    #[serde(rename = "FileList")] file_list: Vec<String>,
    #[serde(rename = "DirList")] dir_list: Vec<String>,
  },
  #[serde(rename = "2")]
  V2 {
    revision: String,
    update_in_progress: bool,
    file_list: Vec<String>,
    dir_list: Vec<String>,
    pending_removal_dir_list: Vec<String>,
  },
}

/// Find the shortest path accross packages
///
/// Returns [`Some(Vec<P>)`] if a path between `start` and `goal` exists
/// Otherwise returns [`None`]
pub fn shortest_path<'a: 'b, 'b, P>(
  start: &'b str,
  goal: &'b str,
  packages: &'a [P],
) -> Option<Vec<&'a P>>
where
  P: Package,
{
  let mut nodes: Vec<Vec<dijkstra::Edge>> = Vec::new();
  let mut name_to_idx: BTreeMap<&'b str, usize> = BTreeMap::new();
  let mut idx_to_name: Vec<&'b str> = Vec::new();
  let mut get_node_idx = |nodes: &mut Vec<Vec<dijkstra::Edge>>,
                          idx_to_name: &mut Vec<&'b str>,
                          name: &'b str|
   -> usize {
    let idx = name_to_idx.entry(name).or_insert_with(|| {
      let idx = nodes.len();
      nodes.push(Vec::new());
      idx_to_name.push(name);
      idx
    });
    *idx
  };
  let empty_idx = get_node_idx(&mut nodes, &mut idx_to_name, "");
  let start_idx = get_node_idx(&mut nodes, &mut idx_to_name, start);
  let goal_idx = get_node_idx(&mut nodes, &mut idx_to_name, goal);
  if empty_idx != start_idx {
    nodes[start_idx].push(dijkstra::Edge {
      node: empty_idx,
      cost: 0,
    });
  }
  for package in packages {
    let from = get_node_idx(&mut nodes, &mut idx_to_name, &package.from());
    let to = get_node_idx(&mut nodes, &mut idx_to_name, &package.to());
    nodes[from].push(dijkstra::Edge {
      node: to,
      cost: package.size(),
    });
  }

  let path = dijkstra::shortest_path(&nodes, start_idx, goal_idx);
  match path {
    Some(path) => {
      let mut path = path.as_slice();
      let mut ret = Vec::new();
      let mut from = start;
      if empty_idx != start_idx && path[0] == empty_idx {
        from = "";
        path = &path[1..];
      }
      for &p in path {
        let to = idx_to_name[p];
        let p = packages
          .iter()
          .find(|&package| package.from() == from && package.to() == to);
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
