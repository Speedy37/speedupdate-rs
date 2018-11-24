use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::usize;

#[derive(Copy, Clone, Eq, PartialEq)]
struct State {
  cost: u64,
  position: usize,
}

// The priority queue depends on `Ord`.
// Explicitly implement the trait so the queue becomes a min-heap
// instead of a max-heap.
impl Ord for State {
  fn cmp(&self, other: &State) -> Ordering {
    // Notice that the we flip the ordering on costs.
    // In case of a tie we compare positions - this step is necessary
    // to make implementations of `PartialEq` and `Ord` consistent.
    other
      .cost
      .cmp(&self.cost)
      .then_with(|| self.position.cmp(&other.position))
  }
}

// `PartialOrd` needs to be implemented as well.
impl PartialOrd for State {
  fn partial_cmp(&self, other: &State) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

// Each node is represented as an `usize`, for a shorter implementation.
pub struct Edge {
  pub node: usize,
  pub cost: u64,
}

struct Track {
  dist: u64,
  prev: Option<usize>,
}

// Dijkstra's shortest path algorithm.

// Start at `start` and use `dist` to track the current shortest distance
// to each node. This implementation isn't memory-efficient as it may leave duplicate
// nodes in the queue. It also uses `usize::MAX` as a sentinel value,
// for a simpler implementation.
pub fn shortest_path(adj_list: &Vec<Vec<Edge>>, start: usize, goal: usize) -> Option<Vec<usize>> {
  // dist[node] = current shortest distance from `start` to `node`
  let mut track: Vec<Track> = (0..adj_list.len())
    .map(|_| Track {
      dist: u64::max_value(),
      prev: None,
    })
    .collect();

  let mut heap = BinaryHeap::new();

  // We're at `start`, with a zero cost
  track[start].dist = 0;
  heap.push(State {
    cost: 0,
    position: start,
  });

  // Examine the frontier with lower cost nodes first (min-heap)
  while let Some(State { cost, position }) = heap.pop() {
    // Alternatively we could have continued to find all shortest paths
    if position == goal {
      let mut path: Vec<usize> = Vec::new();
      let mut p = goal;
      while let Some(prev) = track[p].prev {
        path.push(p);
        p = prev;
      }
      path.reverse();
      assert!(path.len() > 0);
      return Some(path);
    }

    // Important as we may have already found a better way
    if cost > track[position].dist {
      continue;
    }

    // For each node we can reach, see if we can find a way with
    // a lower cost going through this node
    for edge in &adj_list[position] {
      let next = State {
        cost: cost + edge.cost,
        position: edge.node,
      };

      // If so, add it to the frontier and continue
      if next.cost < track[next.position].dist {
        heap.push(next);
        // Relaxation, we have now found a better way
        track[next.position].dist = next.cost;
        track[next.position].prev = Some(position);
      }
    }
  }

  // Goal not reachable
  None
}
