use std::time::{Duration, Instant};

/// Measures the saturation (percentage of time spent working vs
/// waiting for work) of an event processing loop, such as runFSM. It reports the
/// saturation as a gauge metric (at most) once every reportInterval.
///
/// Callers must instrument their loop with calls to sleeping and working, starting
/// with a call to sleeping.
///
/// Note: the caller must be single-threaded and saturationMetric is not safe for
/// concurrent use by multiple task.
pub(crate) struct SaturationMetric {
  name: &'static str,

  report_interval: Duration,

  /// slept contains time for which the event processing loop was sleeping rather
  /// than working in the period since lastReport.
  slept: Duration,

  /// lost contains time that is considered lost due to incorrect use of
  /// saturationMetricBucket (e.g. calling sleeping() or working() multiple
  /// times in succession) in the period since lastReport.
  lost: Duration,

  last_report: Instant,
  sleep_began: Option<Instant>,
  work_began: Option<Instant>,
}

impl SaturationMetric {
  /// Creates a saturation metric that will update the gauge
  /// with the given name at the given report nterval. keepPrev determines the
  /// number of previous measurements that will be used to smooth out spikes.
  pub fn new(name: &'static str, report_interval: Duration) -> Self {
    Self {
      name,
      report_interval,
      slept: Duration::ZERO,
      lost: Duration::ZERO,
      last_report: Instant::now(),
      sleep_began: None,
      work_began: None,
    }
  }

  /// Records the time at which the loop began waiting for work. After the
  /// initial call it must always be proceeded by a call to working.
  pub fn sleeping(&mut self) {
    let now = Instant::now();

    if let Some(began) = self.sleep_began {
      self.lost += now - began;
    }

    self.sleep_began = Some(now);
    self.work_began = None;

    self.report();
  }

  /// Records the time at which the loop began working. It must always be
  /// proceeded by a call to sleeping.
  pub fn working(&mut self) {
    let now = Instant::now();

    if let Some(began) = self.work_began {
      // working called twice in succession. Count that time as lost rather than
      // measuring nonsense.
      self.lost += now - began;
    } else {
      match self.sleep_began {
        Some(began) => {
          self.slept += now - began;
        }
        None => {
          // working called without sleeping first. Count that time as lost rather
          // than measuring nonsense.
          self.lost += now - self.last_report;
        }
      }
    }

    self.work_began = Some(now);
    self.sleep_began = None;

    self.report();
  }

  pub fn report(&mut self) {
    let now = Instant::now();
    let time_since_last_report = now - self.last_report;

    if time_since_last_report < self.report_interval {
      return;
    }

    let total = time_since_last_report - self.lost;
    let saturation = if total != Duration::ZERO {
      let tmp = (total - self.slept).as_millis() as f64 / (total.as_millis() as f64);
      ((tmp * 100.0) / 100.0).round()
    } else {
      0.0
    };

    let histogram = metrics::histogram!(self.name);
    histogram.record(saturation);

    self.slept = Duration::ZERO;
    self.lost = Duration::ZERO;
    self.last_report = now;
  }
}
