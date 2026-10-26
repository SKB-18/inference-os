use crate::error::GatewayError;
use crate::models::{Backend, BackendRegistration, BackendState};
use crate::scheduler::circuit_breaker::CircuitBreaker;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use url::Url;

#[derive(Debug, Clone)]
pub struct BackendRegistry {
    backends: Arc<RwLock<HashMap<String, Backend>>>,
    circuit_breakers: Arc<RwLock<HashMap<String, CircuitBreaker>>>,
    allow_local: bool,
}

impl BackendRegistry {
    pub fn new(allow_local: bool) -> Self {
        Self {
            backends: Arc::new(RwLock::new(HashMap::new())),
            circuit_breakers: Arc::new(RwLock::new(HashMap::new())),
            allow_local,
        }
    }

    pub fn validate_url_ssrf(&self, raw_url: &str) -> Result<Url, GatewayError> {
        let parsed = Url::parse(raw_url)
            .map_err(|e| GatewayError::BadRequest(format!("Invalid backend URL format: {}", e)))?;

        let scheme = parsed.scheme();
        if scheme != "http" && scheme != "https" {
            return Err(GatewayError::BadRequest("Backend URL must use http or https scheme".to_string()));
        }

        let host = parsed.host_str().ok_or_else(|| {
            GatewayError::BadRequest("Backend URL missing host".to_string())
        })?;

        // 1. Strict Cloud Metadata Deny
        if host == "169.254.169.254" || host.starts_with("169.254.") || host == "metadata.google.internal" {
            return Err(GatewayError::SsrfViolation("Cloud metadata endpoints are strictly forbidden".to_string()));
        }

        // 2. Loopback checks
        let is_loopback = host == "localhost" 
            || host == "127.0.0.1" 
            || host == "::1" 
            || host == "0.0.0.0";

        if is_loopback && !self.allow_local {
            return Err(GatewayError::SsrfViolation("Loopback endpoints are disabled in production mode".to_string()));
        }

        Ok(parsed)
    }

    pub fn register_backend(&self, reg: BackendRegistration) -> Result<Backend, GatewayError> {
        self.validate_url_ssrf(&reg.url)?;

        let id = format!("backend-{}", uuid::Uuid::new_v4().to_string()[..8].to_string());
        let backend = Backend::new(
            id.clone(),
            reg.name,
            reg.url.trim_end_matches('/').to_string(),
            reg.model_ids,
            reg.concurrency_limit,
            reg.weight,
        );

        let mut backends = self.backends.write();
        let mut cbs = self.circuit_breakers.write();

        backends.insert(id.clone(), backend.clone());
        cbs.insert(id.clone(), CircuitBreaker::default());

        Ok(backend)
    }

    pub fn get_backend(&self, id: &str) -> Option<Backend> {
        self.backends.read().get(id).cloned()
    }

    pub fn list_backends(&self) -> Vec<Backend> {
        self.backends.read().values().cloned().collect()
    }

    pub fn set_state(&self, id: &str, state: BackendState) -> Result<(), GatewayError> {
        let mut backends = self.backends.write();
        if let Some(b) = backends.get_mut(id) {
            b.state = state;
            b.updated_at = chrono::Utc::now();
            Ok(())
        } else {
            Err(GatewayError::BadRequest(format!("Backend '{}' not found", id)))
        }
    }

    pub fn acquire_inflight(&self, id: &str) -> Result<(), GatewayError> {
        let mut backends = self.backends.write();
        let mut cbs = self.circuit_breakers.write();

        if let Some(cb) = cbs.get_mut(id) {
            if !cb.can_execute() {
                return Err(GatewayError::UpstreamError(format!("Circuit breaker open for backend '{}'", id)));
            }
        }

        if let Some(b) = backends.get_mut(id) {
            if b.state == BackendState::Draining {
                return Err(GatewayError::BackendDraining);
            }
            if b.active_inflight >= b.concurrency_limit {
                return Err(GatewayError::QueueOverloaded {
                    current_depth: b.active_inflight,
                    max_depth: b.concurrency_limit,
                });
            }
            b.active_inflight += 1;
            Ok(())
        } else {
            Err(GatewayError::BadRequest(format!("Backend '{}' not found", id)))
        }
    }

    pub fn release_inflight(&self, id: &str, success: bool, ttft_ms: Option<f64>) {
        let mut backends = self.backends.write();
        let mut cbs = self.circuit_breakers.write();

        if let Some(cb) = cbs.get_mut(id) {
            if success {
                cb.on_success();
            } else {
                cb.on_failure();
            }
        }

        if let Some(b) = backends.get_mut(id) {
            if b.active_inflight > 0 {
                b.active_inflight -= 1;
            }
            if success {
                if let Some(t) = ttft_ms {
                    b.record_success(t);
                }
            } else {
                b.record_failure();
            }
        }
    }

    pub fn is_circuit_open(&self, id: &str) -> bool {
        let mut cbs = self.circuit_breakers.write();
        if let Some(cb) = cbs.get_mut(id) {
            !cb.can_execute()
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_backend_success() {
        let registry = BackendRegistry::new(true);
        let reg = BackendRegistration {
            name: "Worker Fast".to_string(),
            url: "http://127.0.0.1:8001".to_string(),
            model_ids: vec!["gpt-4o-mini".to_string()],
            concurrency_limit: 8,
            weight: 1,
        };

        let backend = registry.register_backend(reg).unwrap();
        assert_eq!(backend.name, "Worker Fast");
        assert_eq!(backend.concurrency_limit, 8);
        assert_eq!(registry.list_backends().len(), 1);
    }

    #[test]
    fn test_ssrf_metadata_rejection() {
        let registry = BackendRegistry::new(true);
        assert!(registry.validate_url_ssrf("http://169.254.169.254/latest/meta-data").is_err());
        assert!(registry.validate_url_ssrf("http://metadata.google.internal/computeMetadata").is_err());
    }

    #[test]
    fn test_ssrf_loopback_rejection_in_prod() {
        let prod_registry = BackendRegistry::new(false);
        assert!(prod_registry.validate_url_ssrf("http://127.0.0.1:8001").is_err());
        assert!(prod_registry.validate_url_ssrf("http://localhost:8001").is_err());
    }

    #[test]
    fn test_draining_mode_rejects_inflight() {
        let registry = BackendRegistry::new(true);
        let reg = BackendRegistration {
            name: "Worker Draining".to_string(),
            url: "http://127.0.0.1:8005".to_string(),
            model_ids: vec!["gpt-4o-mini".to_string()],
            concurrency_limit: 4,
            weight: 1,
        };

        let backend = registry.register_backend(reg).unwrap();
        assert!(registry.acquire_inflight(&backend.id).is_ok());

        registry.set_state(&backend.id, BackendState::Draining).unwrap();
        assert!(registry.acquire_inflight(&backend.id).is_err());
    }

    #[test]
    fn test_concurrency_limit_exhaustion() {
        let registry = BackendRegistry::new(true);
        let reg = BackendRegistration {
            name: "Worker Small".to_string(),
            url: "http://127.0.0.1:8006".to_string(),
            model_ids: vec!["gpt-4o-mini".to_string()],
            concurrency_limit: 1,
            weight: 1,
        };

        let backend = registry.register_backend(reg).unwrap();
        assert!(registry.acquire_inflight(&backend.id).is_ok());
        // Second concurrent request must be rejected
        assert!(registry.acquire_inflight(&backend.id).is_err());

        registry.release_inflight(&backend.id, true, Some(45.0));
        assert!(registry.acquire_inflight(&backend.id).is_ok());
    }
}

