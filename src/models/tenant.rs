use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tenant {
    pub id: String,
    pub name: String,
    pub api_key: String,
    pub token_budget: u64,
    #[serde(default)]
    pub tokens_used: u64,
    #[serde(default)]
    pub reserved_tokens: u64,
    #[serde(default = "default_max_inflight")]
    pub max_inflight: usize,
    #[serde(default)]
    pub active_inflight: usize,
    #[serde(default = "default_allowed_models")]
    pub allowed_models: Vec<String>,
    #[serde(default = "chrono::Utc::now")]
    pub created_at: DateTime<Utc>,
}

fn default_max_inflight() -> usize {
    20
}

fn default_allowed_models() -> Vec<String> {
    vec!["*".to_string()]
}

impl Tenant {
    pub fn new(id: String, name: String, api_key: String, token_budget: u64, max_inflight: usize) -> Self {
        Self {
            id,
            name,
            api_key,
            token_budget,
            tokens_used: 0,
            reserved_tokens: 0,
            max_inflight: if max_inflight == 0 { 20 } else { max_inflight },
            active_inflight: 0,
            allowed_models: vec!["*".to_string()],
            created_at: Utc::now(),
        }
    }

    pub fn can_accept_inflight(&self) -> bool {
        self.active_inflight < self.max_inflight
    }

    pub fn has_budget_for(&self, estimated_tokens: u32) -> bool {
        (self.tokens_used + self.reserved_tokens + (estimated_tokens as u64)) <= self.token_budget
    }

    pub fn reserve_tokens(&mut self, estimated: u32) {
        self.reserved_tokens += estimated as u64;
        self.active_inflight += 1;
    }

    pub fn settle_tokens(&mut self, reserved: u32, actual: u32) {
        if self.reserved_tokens >= (reserved as u64) {
            self.reserved_tokens -= reserved as u64;
        } else {
            self.reserved_tokens = 0;
        }
        self.tokens_used += actual as u64;
        if self.active_inflight > 0 {
            self.active_inflight -= 1;
        }
    }

    pub fn release_reservation_on_failure(&mut self, reserved: u32) {
        if self.reserved_tokens >= (reserved as u64) {
            self.reserved_tokens -= reserved as u64;
        } else {
            self.reserved_tokens = 0;
        }
        if self.active_inflight > 0 {
            self.active_inflight -= 1;
        }
    }

    pub fn allows_model(&self, model: &str) -> bool {
        self.allowed_models.iter().any(|m| m == model || m == "*")
    }
}
