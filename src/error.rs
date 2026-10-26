use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenAiErrorWrapper {
    pub error: OpenAiError,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenAiError {
    pub message: String,
    pub r#type: String,
    pub param: Option<String>,
    pub code: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum GatewayError {
    #[error("Authentication required: {0}")]
    Unauthorized(String),

    #[error("Forbidden: {0}")]
    Forbidden(String),

    #[error("Tenant token budget exceeded: {0}")]
    BudgetExceeded(String),

    #[error("Rate limit exceeded for tenant {tenant_id}: max inflight {limit}")]
    RateLimitExceeded { tenant_id: String, limit: usize },

    #[error("Admission queue is full ({current_depth}/{max_depth}). Try again later.")]
    QueueOverloaded { current_depth: usize, max_depth: usize },

    #[error("Queue wait timeout exceeded ({waited_ms}ms)")]
    QueueTimeout { waited_ms: u64 },

    #[error("Invalid request: {0}")]
    BadRequest(String),

    #[error("Backend not found or no candidate available for model '{model}'")]
    NoHealthyBackend { model: String },

    #[error("Backend request failed: {0}")]
    UpstreamError(String),

    #[error("Backend timed out after {timeout_ms}ms")]
    UpstreamTimeout { timeout_ms: u64 },

    #[error("SSRF validation failed: {0}")]
    SsrfViolation(String),

    #[error("Backend is in draining mode and cannot accept new requests")]
    BackendDraining,

    #[error("Internal gateway error: {0}")]
    Internal(String),
}

impl IntoResponse for GatewayError {
    fn into_response(self) -> Response {
        let (status, err_type, err_code) = match &self {
            GatewayError::Unauthorized(_) => (StatusCode::UNAUTHORIZED, "invalid_request_error", "unauthorized"),
            GatewayError::Forbidden(_) => (StatusCode::FORBIDDEN, "invalid_request_error", "forbidden"),
            GatewayError::BudgetExceeded(_) => (StatusCode::TOO_MANY_REQUESTS, "insufficient_quota", "budget_exceeded"),
            GatewayError::RateLimitExceeded { .. } => (StatusCode::TOO_MANY_REQUESTS, "rate_limit_exceeded", "rate_limit_exceeded"),
            GatewayError::QueueOverloaded { .. } => (StatusCode::SERVICE_UNAVAILABLE, "server_error", "queue_overloaded"),
            GatewayError::QueueTimeout { .. } => (StatusCode::GATEWAY_TIMEOUT, "server_error", "queue_timeout"),
            GatewayError::BadRequest(_) => (StatusCode::BAD_REQUEST, "invalid_request_error", "bad_request"),
            GatewayError::NoHealthyBackend { .. } => (StatusCode::SERVICE_UNAVAILABLE, "server_error", "no_healthy_backend"),
            GatewayError::UpstreamError(_) => (StatusCode::BAD_GATEWAY, "server_error", "upstream_error"),
            GatewayError::UpstreamTimeout { .. } => (StatusCode::GATEWAY_TIMEOUT, "server_error", "upstream_timeout"),
            GatewayError::SsrfViolation(_) => (StatusCode::FORBIDDEN, "invalid_request_error", "ssrf_violation"),
            GatewayError::BackendDraining => (StatusCode::SERVICE_UNAVAILABLE, "server_error", "backend_draining"),
            GatewayError::Internal(_) => (StatusCode::INTERNAL_SERVER_ERROR, "server_error", "internal_error"),
        };

        let body = Json(OpenAiErrorWrapper {
            error: OpenAiError {
                message: self.to_string(),
                r#type: err_type.to_string(),
                param: None,
                code: Some(err_code.to_string()),
            },
        });

        (status, body).into_response()
    }
}
