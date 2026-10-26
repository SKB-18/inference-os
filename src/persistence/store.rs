use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct RequestLogEntry {
    pub id: String,
    pub tenant_id: String,
    pub model: String,
    pub backend_id: String,
    pub ttft_ms: f64,
    pub total_latency_ms: f64,
    pub tokens_used: u32,
    pub status: String,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, Default)]
pub struct PersistenceStore {
    request_logs: Arc<RwLock<Vec<RequestLogEntry>>>,
    idempotency_cache: Arc<RwLock<HashMap<String, String>>>, // idempotency_key -> cached JSON response
}

impl PersistenceStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn log_request(&self, entry: RequestLogEntry) {
        let mut logs = self.request_logs.write();
        if logs.len() > 1000 {
            logs.remove(0);
        }
        logs.push(entry);
    }

    pub fn get_recent_requests(&self, limit: usize) -> Vec<RequestLogEntry> {
        let logs = self.request_logs.read();
        logs.iter().rev().take(limit).cloned().collect()
    }

    pub fn get_idempotency(&self, key: &str) -> Option<String> {
        self.idempotency_cache.read().get(key).cloned()
    }

    pub fn set_idempotency(&self, key: String, response_json: String) {
        let mut cache = self.idempotency_cache.write();
        if cache.len() > 500 {
            cache.clear();
        }
        cache.insert(key, response_json);
    }
}
