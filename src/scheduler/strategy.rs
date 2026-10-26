use crate::error::GatewayError;
use crate::models::{Backend, BackendState, CandidateEvaluation, RouteDecisionRecord};
use crate::scheduler::registry::BackendRegistry;
use parking_lot::RwLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoutingPolicy {
    RoundRobin,
    LeastInflight,
    EstimatedQueueDelay,
}

impl RoutingPolicy {
    pub fn from_str_opt(s: Option<&str>) -> Self {
        match s.map(|v| v.to_lowercase()).as_deref() {
            Some("least-inflight") | Some("least_inflight") => RoutingPolicy::LeastInflight,
            Some("queue-delay") | Some("estimated-queue-delay") | Some("eqd") => RoutingPolicy::EstimatedQueueDelay,
            _ => RoutingPolicy::RoundRobin,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            RoutingPolicy::RoundRobin => "round-robin",
            RoutingPolicy::LeastInflight => "least-inflight",
            RoutingPolicy::EstimatedQueueDelay => "estimated-queue-delay",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Scheduler {
    registry: BackendRegistry,
    rr_counter: Arc<AtomicUsize>,
    audit_history: Arc<RwLock<Vec<RouteDecisionRecord>>>,
}

impl Scheduler {
    pub fn new(registry: BackendRegistry) -> Self {
        Self {
            registry,
            rr_counter: Arc::new(AtomicUsize::new(0)),
            audit_history: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub fn schedule(
        &self,
        request_id: &str,
        tenant_id: &str,
        model: &str,
        policy: RoutingPolicy,
        latency_tier: Option<&str>,
    ) -> Result<(Backend, RouteDecisionRecord), GatewayError> {
        let all_backends = self.registry.list_backends();
        let mut decision = RouteDecisionRecord::new(
            request_id.to_string(),
            tenant_id.to_string(),
            model.to_string(),
            policy.as_str().to_string(),
        );

        if all_backends.is_empty() {
            decision.exclusion_reasons.push("No registered backends in cluster".to_string());
            self.record_audit(decision.clone());
            return Err(GatewayError::NoHealthyBackend { model: model.to_string() });
        }

        let mut eligible: Vec<Backend> = Vec::new();

        for b in &all_backends {
            let mut eval = CandidateEvaluation {
                backend_id: b.id.clone(),
                backend_name: b.name.clone(),
                score: 0.0,
                active_inflight: b.active_inflight,
                concurrency_limit: b.concurrency_limit,
                estimated_queue_delay_ms: b.estimated_queue_delay_ms(),
                eligible: false,
            };

            // 1. Model match check
            if !b.supports_model(model) {
                decision.exclusion_reasons.push(format!("Backend '{}' does not support model '{}'", b.id, model));
                decision.candidates.push(eval);
                continue;
            }

            // 2. State checks
            if b.state == BackendState::Draining {
                decision.exclusion_reasons.push(format!("Backend '{}' is currently draining", b.id));
                decision.candidates.push(eval);
                continue;
            }

            if b.state == BackendState::Unhealthy {
                decision.exclusion_reasons.push(format!("Backend '{}' is unhealthy (failures: {})", b.id, b.consecutive_failures));
                decision.candidates.push(eval);
                continue;
            }

            // 3. Circuit breaker check
            if self.registry.is_circuit_open(&b.id) {
                decision.exclusion_reasons.push(format!("Backend '{}' circuit breaker is OPEN", b.id));
                decision.candidates.push(eval);
                continue;
            }

            // 4. Capacity check
            if b.active_inflight >= b.concurrency_limit {
                decision.exclusion_reasons.push(format!("Backend '{}' is at full capacity ({}/{})", b.id, b.active_inflight, b.concurrency_limit));
                decision.candidates.push(eval);
                continue;
            }

            eval.eligible = true;
            decision.candidates.push(eval);
            eligible.push(b.clone());
        }

        if eligible.is_empty() {
            self.record_audit(decision.clone());
            return Err(GatewayError::NoHealthyBackend { model: model.to_string() });
        }

        let selected = match policy {
            RoutingPolicy::RoundRobin => {
                let idx = self.rr_counter.fetch_add(1, Ordering::Relaxed) % eligible.len();
                eligible[idx].clone()
            }
            RoutingPolicy::LeastInflight => {
                eligible
                    .into_iter()
                    .min_by_key(|b| b.active_inflight)
                    .unwrap()
            }
            RoutingPolicy::EstimatedQueueDelay => {
                // Score = (active_inflight / concurrency_limit) * avg_ttft_ms
                // For interactive latency tier, heavily penalize high TTFT
                let is_interactive = latency_tier == Some("interactive");
                eligible
                    .into_iter()
                    .min_by(|a, b| {
                        let mut score_a = a.estimated_queue_delay_ms();
                        let mut score_b = b.estimated_queue_delay_ms();
                        if is_interactive {
                            score_a += a.average_ttft_ms * 0.5;
                            score_b += b.average_ttft_ms * 0.5;
                        }
                        score_a.partial_cmp(&score_b).unwrap_or(std::cmp::Ordering::Equal)
                    })
                    .unwrap()
            }
        };

        decision.selected_backend_id = Some(selected.id.clone());
        decision.selected_backend_url = Some(selected.url.clone());
        self.record_audit(decision.clone());

        Ok((selected, decision))
    }

    fn record_audit(&self, record: RouteDecisionRecord) {
        let mut hist = self.audit_history.write();
        if hist.len() > 500 {
            hist.remove(0);
        }
        hist.push(record);
    }

    pub fn get_recent_audits(&self, limit: usize) -> Vec<RouteDecisionRecord> {
        let hist = self.audit_history.read();
        hist.iter().rev().take(limit).cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::BackendRegistration;

    fn setup_test_scheduler() -> (Scheduler, BackendRegistry, String, String) {
        let registry = BackendRegistry::new(true);
        let b1 = registry.register_backend(BackendRegistration {
            name: "Worker 1".to_string(),
            url: "http://127.0.0.1:8001".to_string(),
            model_ids: vec!["gpt-4o".to_string()],
            concurrency_limit: 10,
            weight: 1,
        }).unwrap();

        let b2 = registry.register_backend(BackendRegistration {
            name: "Worker 2".to_string(),
            url: "http://127.0.0.1:8002".to_string(),
            model_ids: vec!["gpt-4o".to_string()],
            concurrency_limit: 10,
            weight: 1,
        }).unwrap();

        let scheduler = Scheduler::new(registry.clone());
        (scheduler, registry, b1.id, b2.id)
    }

    #[test]
    fn test_round_robin_selection() {
        let (scheduler, _reg, id1, id2) = setup_test_scheduler();

        let (chosen1, _) = scheduler.schedule("req-1", "t1", "gpt-4o", RoutingPolicy::RoundRobin, None).unwrap();
        let (chosen2, _) = scheduler.schedule("req-2", "t1", "gpt-4o", RoutingPolicy::RoundRobin, None).unwrap();

        // Round robin should alternatingly choose different backends
        assert_ne!(chosen1.id, chosen2.id);
        assert!(chosen1.id == id1 || chosen1.id == id2);
    }

    #[test]
    fn test_least_inflight_selection() {
        let (scheduler, reg, id1, id2) = setup_test_scheduler();

        // Put inflight load on worker 1
        reg.acquire_inflight(&id1).unwrap();
        reg.acquire_inflight(&id1).unwrap();

        // Least inflight policy must pick worker 2 with 0 inflight
        let (chosen, _) = scheduler.schedule("req-1", "t1", "gpt-4o", RoutingPolicy::LeastInflight, None).unwrap();
        assert_eq!(chosen.id, id2);
    }

    #[test]
    fn test_no_healthy_backend_raises() {
        let (scheduler, reg, id1, id2) = setup_test_scheduler();
        reg.set_state(&id1, BackendState::Draining).unwrap();
        reg.set_state(&id2, BackendState::Draining).unwrap();

        let result = scheduler.schedule("req-1", "t1", "gpt-4o", RoutingPolicy::RoundRobin, None);
        assert!(result.is_err());
    }
}

