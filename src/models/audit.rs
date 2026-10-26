use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CandidateEvaluation {
    pub backend_id: String,
    pub backend_name: String,
    pub score: f64,
    pub active_inflight: usize,
    pub concurrency_limit: usize,
    pub estimated_queue_delay_ms: f64,
    pub eligible: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouteDecisionRecord {
    pub request_id: String,
    pub tenant_id: String,
    pub model: String,
    pub policy: String,
    pub selected_backend_id: Option<String>,
    pub selected_backend_url: Option<String>,
    pub candidates: Vec<CandidateEvaluation>,
    pub exclusion_reasons: Vec<String>,
    pub selected_at: DateTime<Utc>,
}

impl RouteDecisionRecord {
    pub fn new(request_id: String, tenant_id: String, model: String, policy: String) -> Self {
        Self {
            request_id,
            tenant_id,
            model,
            policy,
            selected_backend_id: None,
            selected_backend_url: None,
            candidates: Vec::new(),
            exclusion_reasons: Vec::new(),
            selected_at: Utc::now(),
        }
    }
}
