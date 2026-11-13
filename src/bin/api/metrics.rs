use std::sync::Mutex;
const BUCKETS: [f64; 8] = [1., 5., 10., 25., 100., 500., 1000., 10000.];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn metrics_count_errors_histogram_and_storage_stages() {
        let m = Metrics::default();
        m.observe(200, None, 4., 1., None);
        m.observe(503, Some("storage_busy"), 20., 2., None);
        m.observe(504, Some("timeout"), 20000., 10000., None);
        m.runtime_gap(150.);
        m.runtime_gap(10.);
        let s = m.render(3, 1);
        for expected in [
            "reconcile_requests_total 3",
            "reconcile_errors_total 2",
            "reconcile_storage_errors_total 1",
            "reconcile_timeouts_total 1",
            "reconcile_request_duration_ms_bucket{le=\"5\"} 1",
            "reconcile_request_duration_ms_bucket{le=\"+Inf\"} 3",
            "reconcile_storage_queue_depth 3",
            "reconcile_runtime_max_wakeup_lag_ms 150",
        ] {
            assert!(s.contains(expected), "missing {expected}");
        }
    }
}
#[derive(Default)]
pub struct Metrics {
    inner: Mutex<Values>,
}
#[derive(Default)]
struct Values {
    requests: u64,
    errors: u64,
    rejected: u64,
    timeouts: u64,
    storage_errors: u64,
    duration: f64,
    admission: f64,
    buckets: [u64; 8],
    queue: f64,
    dispatch: f64,
    service: f64,
    commit: f64,
    resume: f64,
    timed_events: u64,
    runtime_gap: f64,
}
impl Metrics {
    pub fn observe(
        &self,
        status: u16,
        code: Option<&str>,
        ms: f64,
        admission: f64,
        stages: Option<&super::routes::event_queue::EventStages>,
    ) {
        let mut v = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        v.requests += 1;
        v.duration += ms;
        v.admission += admission;
        v.errors += u64::from(status >= 400);
        v.rejected += u64::from(status == 429 || status == 503);
        v.timeouts += u64::from(status == 504);
        v.storage_errors +=
            u64::from(code == Some("storage_error") || code == Some("storage_busy"));
        for (i, bound) in BUCKETS.iter().enumerate() {
            if ms <= *bound {
                v.buckets[i] += 1;
            }
        }
        if let Some(s) = stages {
            v.timed_events += 1;
            v.queue += s.queue_ms;
            v.dispatch += s.dispatch_ms;
            v.service += s.service_ms;
            v.commit += s.storage.commit_ms;
            v.resume += s.resume_ms;
        }
    }
    pub fn runtime_gap(&self, ms: f64) {
        let mut v = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        v.runtime_gap = v.runtime_gap.max(ms);
    }
    pub fn render(&self, queued: usize, active: usize) -> String {
        let v = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let mut s = String::new();
        for (name, value) in [
            ("requests_total", v.requests),
            ("errors_total", v.errors),
            ("rejected_total", v.rejected),
            ("timeouts_total", v.timeouts),
            ("storage_errors_total", v.storage_errors),
            ("timed_events_total", v.timed_events),
        ] {
            s += &format!("# TYPE reconcile_{name} counter\nreconcile_{name} {value}\n");
        }
        s += "# TYPE reconcile_request_duration_ms histogram\n";
        for (i, bound) in BUCKETS.iter().enumerate() {
            s += &format!(
                "reconcile_request_duration_ms_bucket{{le=\"{bound}\"}} {}\n",
                v.buckets[i]
            );
        }
        s += &format!(
            "reconcile_request_duration_ms_bucket{{le=\"+Inf\"}} {}\nreconcile_request_duration_ms_count {}\nreconcile_request_duration_ms_sum {}\n",
            v.requests, v.requests, v.duration
        );
        for (name, value) in [
            ("admission_ms_total", v.admission),
            ("queue_ms_total", v.queue),
            ("dispatch_ms_total", v.dispatch),
            ("service_ms_total", v.service),
            ("commit_ms_total", v.commit),
            ("resume_ms_total", v.resume),
        ] {
            s += &format!("# TYPE reconcile_{name} counter\nreconcile_{name} {value}\n");
        }
        for (name, value) in [
            ("storage_queue_depth", queued as f64),
            ("storage_active", active as f64),
            ("runtime_max_wakeup_lag_ms", v.runtime_gap),
        ] {
            s += &format!("# TYPE reconcile_{name} gauge\nreconcile_{name} {value}\n");
        }
        s
    }
}
