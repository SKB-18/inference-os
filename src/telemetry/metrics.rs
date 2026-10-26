use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use parking_lot::RwLock;
use std::collections::HashMap;

#[derive(Debug, Default)]
pub struct MetricsState {
    pub total_requests: AtomicU64,
    pub successful_requests: AtomicU64,
    pub failed_requests: AtomicU64,
    pub retried_requests: AtomicU64,
    pub queue_timeouts: AtomicU64,
    pub client_cancellations: AtomicU64,
    pub total_prompt_tokens: AtomicU64,
    pub total_completion_tokens: AtomicU64,
    pub current_queue_depth: AtomicUsize,
    
    // Latency histograms (approximated buckets for Prometheus)
    pub ttft_samples: Arc<RwLock<Vec<f64>>>,
    pub latency_samples: Arc<RwLock<Vec<f64>>>,
    pub backend_served_counts: Arc<RwLock<HashMap<String, u64>>>,
}

impl MetricsState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_ttft(&self, ttft_ms: f64) {
        let mut samples = self.ttft_samples.write();
        if samples.len() > 1000 {
            samples.remove(0);
        }
        samples.push(ttft_ms);
    }

    pub fn record_latency(&self, latency_ms: f64) {
        let mut samples = self.latency_samples.write();
        if samples.len() > 1000 {
            samples.remove(0);
        }
        samples.push(latency_ms);
    }

    pub fn record_backend_served(&self, backend_id: &str) {
        let mut map = self.backend_served_counts.write();
        *map.entry(backend_id.to_string()).or_insert(0) += 1;
    }

    pub fn percentile(&self, is_ttft: bool, p: f64) -> f64 {
        let samples = if is_ttft {
            self.ttft_samples.read().clone()
        } else {
            self.latency_samples.read().clone()
        };

        if samples.is_empty() {
            return 0.0;
        }

        let mut sorted = samples;
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let idx = (((sorted.len() as f64) * p / 100.0) as usize).min(sorted.len() - 1);
        sorted[idx]
    }

    pub fn render_prometheus(&self) -> String {
        let mut out = String::new();
        out.push_str("# HELP inferenceos_requests_total Total requests processed\n");
        out.push_str("# TYPE inferenceos_requests_total counter\n");
        out.push_str(&format!("inferenceos_requests_total {}\n", self.total_requests.load(Ordering::Relaxed)));

        out.push_str("# HELP inferenceos_requests_successful Successful completions\n");
        out.push_str("# TYPE inferenceos_requests_successful counter\n");
        out.push_str(&format!("inferenceos_requests_successful {}\n", self.successful_requests.load(Ordering::Relaxed)));

        out.push_str("# HELP inferenceos_requests_failed Failed requests\n");
        out.push_str("# TYPE inferenceos_requests_failed counter\n");
        out.push_str(&format!("inferenceos_requests_failed {}\n", self.failed_requests.load(Ordering::Relaxed)));

        out.push_str("# HELP inferenceos_requests_retried Safe pre-stream retried requests\n");
        out.push_str("# TYPE inferenceos_requests_retried counter\n");
        out.push_str(&format!("inferenceos_requests_retried {}\n", self.retried_requests.load(Ordering::Relaxed)));

        out.push_str("# HELP inferenceos_client_cancellations Client aborted requests\n");
        out.push_str("# TYPE inferenceos_client_cancellations counter\n");
        out.push_str(&format!("inferenceos_client_cancellations {}\n", self.client_cancellations.load(Ordering::Relaxed)));

        out.push_str("# HELP inferenceos_queue_depth Current requests waiting in admission queue\n");
        out.push_str("# TYPE inferenceos_queue_depth gauge\n");
        out.push_str(&format!("inferenceos_queue_depth {}\n", self.current_queue_depth.load(Ordering::Relaxed)));

        out.push_str("# HELP inferenceos_prompt_tokens_total Total prompt tokens processed\n");
        out.push_str("# TYPE inferenceos_prompt_tokens_total counter\n");
        out.push_str(&format!("inferenceos_prompt_tokens_total {}\n", self.total_prompt_tokens.load(Ordering::Relaxed)));

        out.push_str("# HELP inferenceos_completion_tokens_total Total completion tokens served\n");
        out.push_str("# TYPE inferenceos_completion_tokens_total counter\n");
        out.push_str(&format!("inferenceos_completion_tokens_total {}\n", self.total_completion_tokens.load(Ordering::Relaxed)));

        out.push_str("# HELP inferenceos_ttft_p50_ms Estimated p50 TTFT\n");
        out.push_str(&format!("inferenceos_ttft_p50_ms {:.2}\n", self.percentile(true, 50.0)));
        out.push_str("# HELP inferenceos_ttft_p95_ms Estimated p95 TTFT\n");
        out.push_str(&format!("inferenceos_ttft_p95_ms {:.2}\n", self.percentile(true, 95.0)));
        out.push_str("# HELP inferenceos_ttft_p99_ms Estimated p99 TTFT\n");
        out.push_str(&format!("inferenceos_ttft_p99_ms {:.2}\n", self.percentile(true, 99.0)));

        let counts = self.backend_served_counts.read();
        out.push_str("# HELP inferenceos_backend_requests_total Requests served per backend\n");
        out.push_str("# TYPE inferenceos_backend_requests_total counter\n");
        for (b, count) in counts.iter() {
            out.push_str(&format!("inferenceos_backend_requests_total{{backend=\"{}\"}} {}\n", b, count));
        }

        out
    }
}
