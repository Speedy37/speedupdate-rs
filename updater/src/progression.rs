use std::collections::VecDeque;
use std::ops::{Add, AddAssign, Range, Sub, SubAssign};
use std::time::{Duration, Instant};

#[derive(Debug)]
pub struct TimedProgression {
  speed: ProgressionOverTime,
  last_instant: Instant,
  last_progression: Progression,
  history: VecDeque<(Duration, Progression)>,
  steps: usize,
  step_min_duration: Duration,
}

impl TimedProgression {
  pub fn new(steps: usize, duration: Duration) -> TimedProgression {
    TimedProgression {
      speed: ProgressionOverTime(Duration::new(0, 0), Progression::new()),
      last_instant: Instant::now(),
      last_progression: Progression::new(),
      history: VecDeque::with_capacity(steps),
      steps,
      step_min_duration: duration / (steps as u32),
    }
  }

  pub fn add(&mut self, progression: Progression) -> Option<&ProgressionOverTime> {
    let now = Instant::now();
    let elapsed = now.duration_since(self.last_instant);
    let delta = &progression - &self.last_progression;
    self.last_instant = now;
    self.last_progression = progression;
    self.speed.0 += elapsed;
    self.speed.1 += &delta;

    let is_significant = match self.history.back_mut() {
      Some(&mut (ref mut duration, ref mut old_progression))
        if *duration < self.step_min_duration =>
      {
        *duration += elapsed;
        *old_progression += &delta;
        false
      }
      _ => true,
    };
    if is_significant {
      if self.history.len() == self.steps {
        let front = self.history.pop_front().unwrap();
        self.speed.0 -= front.0;
        self.speed.1 -= &front.1;
      }
      self.history.push_back((elapsed, delta));
      Some(&self.speed)
    } else {
      None
    }
  }
}

#[derive(Debug)]
pub struct ProgressionOverTime(Duration, Progression);

impl ProgressionOverTime {
  pub fn duration_as_secs(&self) -> f64 {
    (self.0.as_secs() as f64) + (self.0.subsec_nanos() as f64 * 1e-9)
  }

  pub fn downloaded_files_per_sec(&self) -> f64 {
    (self.1.downloaded_files as f64) / self.duration_as_secs()
  }
  pub fn downloaded_bytes_per_sec(&self) -> f64 {
    (self.1.downloaded_bytes as f64) / self.duration_as_secs()
  }

  pub fn applied_files_per_sec(&self) -> f64 {
    (self.1.applied_files as f64) / self.duration_as_secs()
  }
  pub fn applied_input_bytes_per_sec(&self) -> f64 {
    (self.1.applied_input_bytes as f64) / self.duration_as_secs()
  }
  pub fn applied_output_bytes_per_sec(&self) -> f64 {
    (self.1.applied_output_bytes as f64) / self.duration_as_secs()
  }
}

#[derive(Debug, Clone)]
pub struct GlobalProgression {
  pub packages: Range<usize>,

  pub downloaded_files: Range<usize>,
  pub downloaded_bytes: Range<u64>,

  pub applied_files: Range<usize>,
  pub applied_input_bytes: Range<u64>,
  pub applied_output_bytes: Range<u64>,

  pub failed_files: usize,
}

impl<'a> AddAssign<&'a Progression> for GlobalProgression {
  fn add_assign(&mut self, other: &'a Progression) {
    self.downloaded_files.start += other.downloaded_files;
    self.downloaded_bytes.start += other.downloaded_bytes;

    self.applied_files.start += other.applied_files;
    self.applied_input_bytes.start += other.applied_input_bytes;
    self.applied_output_bytes.start += other.applied_output_bytes;

    self.failed_files += other.failed_files;
  }
}

impl GlobalProgression {
  pub fn new() -> GlobalProgression {
    GlobalProgression {
      packages: Range { start: 0, end: 0 },

      downloaded_files: Range { start: 0, end: 0 },
      downloaded_bytes: Range { start: 0, end: 0 },

      applied_files: Range { start: 0, end: 0 },
      applied_input_bytes: Range { start: 0, end: 0 },
      applied_output_bytes: Range { start: 0, end: 0 },

      failed_files: 0,
    }
  }
}

#[derive(Debug, Clone)]
pub struct Progression {
  pub downloaded_files: usize,
  pub downloaded_bytes: u64,

  pub applied_files: usize,
  pub applied_input_bytes: u64,
  pub applied_output_bytes: u64,

  pub failed_files: usize,
}

impl<'a> From<&'a GlobalProgression> for Progression {
  fn from(global_progression: &GlobalProgression) -> Progression {
    Progression {
      downloaded_files: global_progression.downloaded_files.start,
      downloaded_bytes: global_progression.downloaded_bytes.start,

      applied_files: global_progression.applied_files.start,
      applied_input_bytes: global_progression.applied_input_bytes.start,
      applied_output_bytes: global_progression.applied_output_bytes.start,

      failed_files: global_progression.failed_files,
    }
  }
}

impl Progression {
  pub fn new() -> Progression {
    Progression {
      downloaded_files: 0,
      downloaded_bytes: 0,

      applied_files: 0,
      applied_input_bytes: 0,
      applied_output_bytes: 0,

      failed_files: 0,
    }
  }
}

impl<'a, 'b> Add<&'a Progression> for &'b Progression {
  type Output = Progression;

  fn add(self, other: &'a Progression) -> Progression {
    Progression {
      downloaded_files: self.downloaded_files + other.downloaded_files,
      downloaded_bytes: self.downloaded_bytes + other.downloaded_bytes,

      applied_files: self.applied_files + other.applied_files,
      applied_input_bytes: self.applied_input_bytes + other.applied_input_bytes,
      applied_output_bytes: self.applied_output_bytes + other.applied_output_bytes,

      failed_files: self.failed_files + other.failed_files,
    }
  }
}

impl<'a> AddAssign<&'a Progression> for Progression {
  fn add_assign(&mut self, other: &'a Progression) {
    self.downloaded_files += other.downloaded_files;
    self.downloaded_bytes += other.downloaded_bytes;

    self.applied_files += other.applied_files;
    self.applied_input_bytes += other.applied_input_bytes;
    self.applied_output_bytes += other.applied_output_bytes;

    self.failed_files += other.failed_files;
  }
}

impl<'a, 'b> Sub<&'a Progression> for &'b Progression {
  type Output = Progression;

  fn sub(self, other: &'a Progression) -> Progression {
    Progression {
      downloaded_files: self.downloaded_files - other.downloaded_files,
      downloaded_bytes: self.downloaded_bytes - other.downloaded_bytes,

      applied_files: self.applied_files - other.applied_files,
      applied_input_bytes: self.applied_input_bytes - other.applied_input_bytes,
      applied_output_bytes: self.applied_output_bytes - other.applied_output_bytes,

      failed_files: self.failed_files - other.failed_files,
    }
  }
}

impl<'a> SubAssign<&'a Progression> for Progression {
  fn sub_assign(&mut self, other: &'a Progression) {
    self.downloaded_files -= other.downloaded_files;
    self.downloaded_bytes -= other.downloaded_bytes;

    self.applied_files -= other.applied_files;
    self.applied_input_bytes -= other.applied_input_bytes;
    self.applied_output_bytes -= other.applied_output_bytes;

    self.failed_files -= other.failed_files;
  }
}
