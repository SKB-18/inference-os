use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BackendState {
    Healthy,
    Degraded,
    Unhealthy,
    Draining,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Backend {
    pub id: String,
    pub name: String,
    pub url: String,
    pub model_ids: Vec<String>,
    pub concurrency_limit: usize,
    pub active_inflight: usize,
    pub weight: u32,
    pub state: BackendState,
    pub average_ttft_ms: f64,
    pub total_served: u64,
    pub total_errors: u64,
    pub consecutive_failures: u32,
    pub last_health_check: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}

impl Backend {
    pub fn new(id: String, name: String, url: String, model_ids: Vec<String>, concurrency_limit: usize, weight: u32) -> Self {
        Self {
            id,
            name,
            url,
            model_ids,
            concurrency_limit: if concurrency_limit == 0 { 10 } else { concurrency_limit },
            active_inflight: 0,
            weight: if weight == 0 { 1 } else { weight },
            state: BackendState::Healthy,
            average_ttft_ms: 50.0,
            total_served: 0,
            total_errors: 0,
            consecutive_failures: 0,
            last_health_check: Some(Utc::now()),
            updated_at: Utc::now(),
        }
    }

    pub fn supports_model(&self, model: &str) -> bool {
        self.model_ids.iter().any(|m| m == model || m == "*")
    }

    pub fn is_admissible(&self) -> bool {
        (self.state == BackendState::Healthy || self.state == BackendState::Degraded) 
            && self.active_inflight < self.concurrency_limit
    }

    pub fn estimated_queue_delay_ms(&self) -> f64 {
        let queue_ratio = (self.active_inflight as f64) / (self.concurrency_limit as f64);
        self.average_ttft_ms * queue_ratio
    }

    pub fn record_success(&mut self, ttft_ms: f64) {
        self.total_served += 1;
        self.consecutive_failures = 0;
        // Exponential moving average for TTFT (alpha = 0.2)
        self.average_ttft_ms = (0.8 * self.average_ttft_ms) + (0.2 * ttft_ms);
        if self.state == BackendState::Degraded || self.state == BackendState::Unhealthy {
            self.state = BackendState::Healthy;
        }
        self.updated_at = Utc::now();
    }

    pub fn record_failure(&mut self) {
        self.total_errors += 1;
        self.consecutive_failures += 1;
        if self.consecutive_failures >= 3 && self.state != BackendState::Draining {
            self.state = BackendState::Unhealthy;
        } else if self.consecutive_failures >= 1 && self.state == BackendState::Healthy {
            self.state = BackendState::Degraded;
        }
        self.updated_at = Utc::now();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackendRegistration {
    pub url: String,
    #[serde(default = "default_backend_name")]
    pub name: String,
    pub model_ids: Vec<String>,
    #[serde(default = "default_concurrency_limit")]
    pub concurrency_limit: usize,
    #[serde(default = "default_weight")]
    pub weight: u32,
}

fn default_backend_name() -> String {
    format!("backend-{}", uuid::Uuid::new_v4().to_string()[..8].to_string())
}

fn default_concurrency_limit() -> usize {
    10
}

fn default_weight() -> u32 {
    1
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackendStatusSummary {
    pub backend_id: String,
    pub name: String,
    pub url: String,
    pub status: BackendState,
    pub queue_depth: usize,
    pub concurrency_limit: usize,
    pub capabilities: Vec<String>,
    pub average_ttft_ms: f64,
    pub total_served: u64,
    pub total_errors: u64,
}
