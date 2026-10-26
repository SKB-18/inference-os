use crate::error::GatewayError;
use crate::models::Tenant;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct TenantLimiter {
    tenants_by_key: Arc<RwLock<HashMap<String, Tenant>>>,
    tenants_by_id: Arc<RwLock<HashMap<String, Tenant>>>,
}

impl TenantLimiter {
    pub fn new(default_budget: u64, default_max_inflight: usize) -> Self {
        let instance = Self {
            tenants_by_key: Arc::new(RwLock::new(HashMap::new())),
            tenants_by_id: Arc::new(RwLock::new(HashMap::new())),
        };

        // Bootstrap default demo tenant
        let default_tenant = Tenant::new(
            "tenant-demo".to_string(),
            "Demo Engineering Team".to_string(),
            "sk-inferenceos-demo-key".to_string(),
            default_budget,
            default_max_inflight,
        );
        instance.register_tenant(default_tenant);

        // Bootstrap high-volume test tenant
        let test_tenant = Tenant::new(
            "tenant-qa".to_string(),
            "QA Automation Fleet".to_string(),
            "sk-inferenceos-qa-key".to_string(),
            default_budget,
            default_max_inflight,
        );
        instance.register_tenant(test_tenant);

        instance
    }

    pub fn register_tenant(&self, tenant: Tenant) {
        let mut by_key = self.tenants_by_key.write();
        let mut by_id = self.tenants_by_id.write();

        by_key.insert(tenant.api_key.clone(), tenant.clone());
        by_id.insert(tenant.id.clone(), tenant);
    }

    pub fn authenticate(&self, bearer_token: &str) -> Result<Tenant, GatewayError> {
        let clean_token = bearer_token.trim().trim_start_matches("Bearer ").trim();
        let by_key = self.tenants_by_key.read();

        if let Some(t) = by_key.get(clean_token) {
            Ok(t.clone())
        } else {
            Err(GatewayError::Unauthorized("Invalid API Bearer key".to_string()))
        }
    }

    pub fn reserve_budget(&self, tenant_id: &str, estimated_tokens: u32) -> Result<(), GatewayError> {
        let mut by_id = self.tenants_by_id.write();
        let mut by_key = self.tenants_by_key.write();

        if let Some(t) = by_id.get_mut(tenant_id) {
            // Check concurrency limit
            if !t.can_accept_inflight() {
                return Err(GatewayError::RateLimitExceeded {
                    tenant_id: tenant_id.to_string(),
                    limit: t.max_inflight,
                });
            }

            // Check token budget limit
            if !t.has_budget_for(estimated_tokens) {
                return Err(GatewayError::BudgetExceeded(format!(
                    "Remaining budget {} is less than estimated {} tokens",
                    t.token_budget.saturating_sub(t.tokens_used + t.reserved_tokens),
                    estimated_tokens
                )));
            }

            t.reserve_tokens(estimated_tokens);

            // Synchronize by_key entry
            if let Some(k) = by_key.get_mut(&t.api_key) {
                *k = t.clone();
            }

            Ok(())
        } else {
            Err(GatewayError::Unauthorized(format!("Tenant '{}' not found", tenant_id)))
        }
    }

    pub fn settle_budget(&self, tenant_id: &str, reserved: u32, actual: u32) {
        let mut by_id = self.tenants_by_id.write();
        let mut by_key = self.tenants_by_key.write();

        if let Some(t) = by_id.get_mut(tenant_id) {
            t.settle_tokens(reserved, actual);
            if let Some(k) = by_key.get_mut(&t.api_key) {
                *k = t.clone();
            }
        }
    }

    pub fn release_budget_on_failure(&self, tenant_id: &str, reserved: u32) {
        let mut by_id = self.tenants_by_id.write();
        let mut by_key = self.tenants_by_key.write();

        if let Some(t) = by_id.get_mut(tenant_id) {
            t.release_reservation_on_failure(reserved);
            if let Some(k) = by_key.get_mut(&t.api_key) {
                *k = t.clone();
            }
        }
    }

    pub fn list_tenants(&self) -> Vec<Tenant> {
        self.tenants_by_id.read().values().cloned().collect()
    }
}
