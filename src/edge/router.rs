use crate::admission::estimator::TokenEstimator;
use crate::admission::limiter::TenantLimiter;
use crate::admission::queue::AdmissionQueue;
use crate::config::AppConfig;
use crate::edge::auth::AuthMiddleware;
use crate::edge::streaming::StreamEngine;
use crate::error::GatewayError;
use crate::models::{
    BackendRegistration, BackendState, BackendStatusSummary, ChatCompletionRequest,
    RouteDecisionRecord, Tenant,
};
use crate::persistence::store::PersistenceStore;
use crate::scheduler::registry::BackendRegistry;
use crate::scheduler::strategy::{RoutingPolicy, Scheduler};
use crate::telemetry::metrics::MetricsState;
use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tracing::{error, info, warn};

#[derive(Clone)]
pub struct AppState {
    pub config: AppConfig,
    pub registry: BackendRegistry,
    pub scheduler: Scheduler,
    pub limiter: TenantLimiter,
    pub queue: AdmissionQueue,
    pub stream_engine: Arc<StreamEngine>,
    pub persistence: Arc<PersistenceStore>,
    pub metrics: Arc<MetricsState>,
}

pub fn create_router(state: AppState) -> Router {
    Router::new()
        .route("/v1/chat/completions", post(handle_chat_completions))
        .route("/admin/backends", get(handle_list_backends).post(handle_register_backend))
        .route("/admin/backends/:id/drain", post(handle_drain_backend))
        .route("/admin/tenants", get(handle_list_tenants).post(handle_register_tenant))
        .route("/admin/routes/audit", get(handle_audit_history))
        .route("/healthz", get(handle_healthz))
        .route("/readyz", get(handle_readyz))
        .route("/metrics", get(handle_metrics))
        .with_state(state)
}

/// Unified Chat Completions Endpoint (/v1/chat/completions)
async fn handle_chat_completions(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<ChatCompletionRequest>,
) -> Result<Response, GatewayError> {
    state.metrics.total_requests.fetch_add(1, Ordering::Relaxed);
    let request_id = format!("req-{}", uuid::Uuid::new_v4().to_string()[..12].to_string());

    // 1. Authenticate Tenant
    let tenant = AuthMiddleware::extract_tenant(&headers, &state.limiter)?;

    // 2. Validate Model Capability
    if !tenant.allows_model(&req.model) {
        return Err(GatewayError::Forbidden(format!(
            "Tenant '{}' is not authorized to access model '{}'",
            tenant.id, req.model
        )));
    }

    // 3. Check Idempotency Key (Non-streaming only)
    if let Some(ref ikey) = req.idempotency_key {
        if !req.stream {
            if let Some(cached) = state.persistence.get_idempotency(ikey) {
                return Ok((
                    StatusCode::OK,
                    [("Content-Type", "application/json"), ("X-InferenceOS-Cached", "true")],
                    cached,
                ).into_response());
            }
        }
    }

    // 4. Token Estimation & Soft Budget Reservation
    let (_prompt_est, total_est) = TokenEstimator::estimate(&req);
    state.limiter.reserve_budget(&tenant.id, total_est)?;

    // 5. Bounded Queue Admission (Backpressure)
    state.metrics.current_queue_depth.store(state.queue.current_waiting_count(), Ordering::Relaxed);
    let _permit = match state.queue.admit().await {
        Ok(permit) => permit,
        Err(e) => {
            state.limiter.release_budget_on_failure(&tenant.id, total_est);
            state.metrics.failed_requests.fetch_add(1, Ordering::Relaxed);
            return Err(e);
        }
    };

    // 6. Routing Strategy Resolution
    let policy_header = headers.get("x-inferenceos-policy").and_then(|h| h.to_str().ok());
    let policy = RoutingPolicy::from_str_opt(policy_header);

    // 7. Schedule Backend Worker
    let (backend, _decision) = match state.scheduler.schedule(
        &request_id,
        &tenant.id,
        &req.model,
        policy,
        req.latency_tier.as_deref(),
    ) {
        Ok(res) => res,
        Err(e) => {
            state.limiter.release_budget_on_failure(&tenant.id, total_est);
            state.metrics.failed_requests.fetch_add(1, Ordering::Relaxed);
            return Err(e);
        }
    };

    // Acquire Inflight on Backend
    if let Err(e) = state.registry.acquire_inflight(&backend.id) {
        state.limiter.release_budget_on_failure(&tenant.id, total_est);
        state.metrics.failed_requests.fetch_add(1, Ordering::Relaxed);
        return Err(e);
    }

    // 8. Execute Execution: Non-streaming vs Streaming with Pre-Stream Safe Retry
    if !req.stream {
        // Non-streaming execution
        match state.stream_engine.execute_non_streaming(&backend, &tenant, &req, total_est, Some(&headers)).await {
            Ok(res) => {
                let serialized = serde_json::to_string(&res).map_err(|e| GatewayError::Internal(e.to_string()))?;
                if let Some(ref ikey) = req.idempotency_key {
                    state.persistence.set_idempotency(ikey.clone(), serialized.clone());
                }
                Ok((
                    StatusCode::OK,
                    [
                        ("Content-Type", "application/json"),
                        ("X-InferenceOS-Backend", backend.name.as_str()),
                        ("X-InferenceOS-Policy", policy.as_str()),
                        ("X-InferenceOS-Request-Id", request_id.as_str()),
                    ],
                    serialized,
                ).into_response())
            }
            Err(first_err) => {
                warn!("Backend '{}' failed on non-streaming: {}. Trying safe retry...", backend.id, first_err);
                state.registry.release_inflight(&backend.id, false, None);

                // Attempt Safe Retry on alternate worker
                if let Ok((second_backend, _)) = state.scheduler.schedule(
                    &request_id,
                    &tenant.id,
                    &req.model,
                    policy,
                    req.latency_tier.as_deref(),
                ) {
                    if second_backend.id != backend.id && state.registry.acquire_inflight(&second_backend.id).is_ok() {
                        state.metrics.retried_requests.fetch_add(1, Ordering::Relaxed);
                        if let Ok(res) = state.stream_engine.execute_non_streaming(&second_backend, &tenant, &req, total_est, None).await {
                            let serialized = serde_json::to_string(&res).map_err(|e| GatewayError::Internal(e.to_string()))?;
                            return Ok((
                                StatusCode::OK,
                                [
                                    ("Content-Type", "application/json"),
                                    ("X-InferenceOS-Backend", second_backend.name.as_str()),
                                    ("X-InferenceOS-Policy", policy.as_str()),
                                    ("X-InferenceOS-Request-Id", request_id.as_str()),
                                    ("X-InferenceOS-Retried", "true"),
                                ],
                                serialized,
                            ).into_response());
                        }
                    }
                }

                state.limiter.release_budget_on_failure(&tenant.id, total_est);
                state.metrics.failed_requests.fetch_add(1, Ordering::Relaxed);
                Err(first_err)
            }
        }
    } else {
        // Streaming execution (SSE)
        match state.stream_engine.execute_streaming(&backend, &tenant, &req, total_est, Some(&headers)).await {
            Ok(sse_res) => {
                let mut resp = sse_res.into_response();
                resp.headers_mut().insert("X-InferenceOS-Backend", backend.name.parse().unwrap());
                resp.headers_mut().insert("X-InferenceOS-Policy", policy.as_str().parse().unwrap());
                resp.headers_mut().insert("X-InferenceOS-Request-Id", request_id.parse().unwrap());
                Ok(resp)
            }
            Err(first_err) => {
                warn!("Backend '{}' failed pre-stream: {}. Executing safe retry...", backend.id, first_err);
                state.registry.release_inflight(&backend.id, false, None);

                // Safe retry since zero tokens have been flushed!
                if let Ok((second_backend, _)) = state.scheduler.schedule(
                    &request_id,
                    &tenant.id,
                    &req.model,
                    policy,
                    req.latency_tier.as_deref(),
                ) {
                    if second_backend.id != backend.id && state.registry.acquire_inflight(&second_backend.id).is_ok() {
                        state.metrics.retried_requests.fetch_add(1, Ordering::Relaxed);
                        if let Ok(sse_res) = state.stream_engine.execute_streaming(&second_backend, &tenant, &req, total_est, None).await {
                            let mut resp = sse_res.into_response();
                            resp.headers_mut().insert("X-InferenceOS-Backend", second_backend.name.parse().unwrap());
                            resp.headers_mut().insert("X-InferenceOS-Policy", policy.as_str().parse().unwrap());
                            resp.headers_mut().insert("X-InferenceOS-Request-Id", request_id.parse().unwrap());
                            resp.headers_mut().insert("X-InferenceOS-Retried", "true".parse().unwrap());
                            return Ok(resp);
                        }
                    }
                }

                state.limiter.release_budget_on_failure(&tenant.id, total_est);
                state.metrics.failed_requests.fetch_add(1, Ordering::Relaxed);
                Err(first_err)
            }
        }
    }
}

/// GET /admin/backends
async fn handle_list_backends(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Vec<BackendStatusSummary>>, GatewayError> {
    AuthMiddleware::verify_admin(&headers, &state.config.admin_api_key)?;
    let backends = state.registry.list_backends();
    let summaries: Vec<BackendStatusSummary> = backends
        .into_iter()
        .map(|b| BackendStatusSummary {
            backend_id: b.id,
            name: b.name,
            url: b.url,
            status: b.state,
            queue_depth: b.active_inflight,
            concurrency_limit: b.concurrency_limit,
            capabilities: b.model_ids,
            average_ttft_ms: b.average_ttft_ms,
            total_served: b.total_served,
            total_errors: b.total_errors,
        })
        .collect();

    Ok(Json(summaries))
}

/// POST /admin/backends
async fn handle_register_backend(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(reg): Json<BackendRegistration>,
) -> Result<(StatusCode, Json<BackendStatusSummary>), GatewayError> {
    AuthMiddleware::verify_admin(&headers, &state.config.admin_api_key)?;
    let backend = state.registry.register_backend(reg)?;
    info!("Registered backend '{}' at {}", backend.id, backend.url);

    let summary = BackendStatusSummary {
        backend_id: backend.id,
        name: backend.name,
        url: backend.url,
        status: backend.state,
        queue_depth: backend.active_inflight,
        concurrency_limit: backend.concurrency_limit,
        capabilities: backend.model_ids,
        average_ttft_ms: backend.average_ttft_ms,
        total_served: backend.total_served,
        total_errors: backend.total_errors,
    };

    Ok((StatusCode::CREATED, Json(summary)))
}

/// POST /admin/backends/:id/drain
async fn handle_drain_backend(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, GatewayError> {
    AuthMiddleware::verify_admin(&headers, &state.config.admin_api_key)?;
    let current = state.registry.get_backend(&id).ok_or_else(|| {
        GatewayError::BadRequest(format!("Backend '{}' not found", id))
    })?;

    let next_state = if current.state == BackendState::Draining {
        BackendState::Healthy
    } else {
        BackendState::Draining
    };

    state.registry.set_state(&id, next_state)?;
    info!("Toggled backend '{}' state to {:?}", id, next_state);

    Ok(Json(serde_json::json!({
        "backend_id": id,
        "new_state": next_state,
        "message": if next_state == BackendState::Draining { "Backend is draining and will reject new dispatches" } else { "Backend returned to healthy service" }
    })))
}

/// GET /admin/tenants
async fn handle_list_tenants(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Vec<Tenant>>, GatewayError> {
    AuthMiddleware::verify_admin(&headers, &state.config.admin_api_key)?;
    Ok(Json(state.limiter.list_tenants()))
}

/// POST /admin/tenants
async fn handle_register_tenant(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(tenant): Json<Tenant>,
) -> Result<(StatusCode, Json<Tenant>), GatewayError> {
    AuthMiddleware::verify_admin(&headers, &state.config.admin_api_key)?;
    state.limiter.register_tenant(tenant.clone());
    Ok((StatusCode::CREATED, Json(tenant)))
}

/// GET /admin/routes/audit
async fn handle_audit_history(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Vec<RouteDecisionRecord>>, GatewayError> {
    AuthMiddleware::verify_admin(&headers, &state.config.admin_api_key)?;
    Ok(Json(state.scheduler.get_recent_audits(50)))
}

/// GET /healthz
async fn handle_healthz() -> &'static str {
    "OK"
}

/// GET /readyz
async fn handle_readyz(State(state): State<AppState>) -> Result<&'static str, StatusCode> {
    let backends = state.registry.list_backends();
    if backends.iter().any(|b| b.state == BackendState::Healthy || b.state == BackendState::Degraded) {
        Ok("READY")
    } else {
        Err(StatusCode::SERVICE_UNAVAILABLE)
    }
}

/// GET /metrics (Prometheus)
async fn handle_metrics(State(state): State<AppState>) -> (StatusCode, [(&'static str, &'static str); 1], String) {
    let body = state.metrics.render_prometheus();
    (StatusCode::OK, [("Content-Type", "text/plain; version=0.0.4; charset=utf-8")], body)
}
