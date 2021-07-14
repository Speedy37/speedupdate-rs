//! Distribution of progression over time
use std::collections::VecDeque;
use std::ops::{AddAssign, Div, SubAssign};
use std::time::{Duration, Instant};

#[derive(Debug, Default)]
pub struct HistogramStep<P> {
    duration: Duration,
    delta: P,
}

#[derive(Debug)]
pub struct Histogram<P> {
    speed: HistogramStep<P>,
    last_instant: Instant,
    last_progression: P,
    history: VecDeque<HistogramStep<P>>,
    steps: usize,
    step_min_duration: Duration,
}

impl<P: Default> Histogram<P> {
    pub fn new(steps: usize, duration: Duration) -> Self {
        Self {
            speed: HistogramStep::default(),
            last_instant: Instant::now(),
            last_progression: P::default(),
            history: VecDeque::with_capacity(steps),
            steps,
            step_min_duration: duration / (steps as u32),
        }
    }
}

impl<P: Default> Default for Histogram<P> {
    fn default() -> Self {
        Self::new(10, Duration::new(2, 0))
    }
}

impl<P> Histogram<P>
where
    P: Clone + for<'a> AddAssign<&'a P> + for<'a> SubAssign<&'a P>,
{
    pub fn add(&mut self, progress: P) -> Option<&HistogramStep<P>> {
        let mut delta = self.last_progression.clone();
        delta -= &progress;
        self.inc(delta)
    }

    pub fn inc(&mut self, delta: P) -> Option<&HistogramStep<P>> {
        let now = Instant::now();
        let duration = now.duration_since(self.last_instant);
        self.last_instant = now;
        self.last_progression += &delta;
        self.speed.duration += duration;
        self.speed.delta += &delta;

        let is_significant = match self.history.back_mut() {
            Some(old_step) if old_step.duration < self.step_min_duration => {
                old_step.duration += duration;
                old_step.delta += &delta;
                false
            }
            _ => true,
        };
        if is_significant {
            if self.history.len() == self.steps {
                let front = self.history.pop_front().unwrap();
                self.speed.duration -= front.duration;
                self.speed.delta -= &front.delta;
            }
            self.history.push_back(HistogramStep { duration, delta });
            Some(&self.speed)
        } else {
            None
        }
    }

    pub fn progress(&self) -> &P {
        &self.last_progression
    }

    pub fn speed(&self) -> &HistogramStep<P> {
        &self.speed
    }
}
impl<P> HistogramStep<P> {
    pub fn duration_as_secs(&self) -> f64 {
        (self.duration.as_secs() as f64) + (self.duration.subsec_nanos() as f64 * 1e-9)
    }
}

impl<'a, P: 'a> HistogramStep<P>
where
    <&'a P as Div<f64>>::Output: Default,
    &'a P: Div<f64>,
{
    pub fn progress_per_sec(&'a self) -> <&'a P as Div<f64>>::Output {
        let d = self.duration_as_secs();
        if d > 0f64 {
            (&self.delta) / d
        } else {
            <&'a P as Div<f64>>::Output::default()
        }
    }
}
