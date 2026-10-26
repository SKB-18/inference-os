use crate::admission::limiter::TenantLimiter;
use crate::error::GatewayError;
use crate::models::Tenant;
use axum::http::HeaderMap;

pub struct AuthMiddleware;

impl AuthMiddleware {
    pub fn extract_tenant(headers: &HeaderMap, limiter: &TenantLimiter) -> Result<Tenant, GatewayError> {
        let auth_header = headers
            .get("Authorization")
            .and_then(|h| h.to_str().ok())
            .ok_or_else(|| GatewayError::Unauthorized("Missing Authorization header".to_string()))?;

        limiter.authenticate(auth_header)
    }

    pub fn verify_admin(headers: &HeaderMap, admin_key: &str) -> Result<(), GatewayError> {
        let auth_header = headers
            .get("Authorization")
            .and_then(|h| h.to_str().ok())
            .ok_or_else(|| GatewayError::Unauthorized("Missing Authorization header for admin".to_string()))?;

        let token = auth_header.trim().trim_start_matches("Bearer ").trim();
        if token == admin_key {
            Ok(())
        } else {
            Err(GatewayError::Forbidden("Invalid admin authorization credentials".to_string()))
        }
    }
}
