mod admission;
mod config;
mod edge;
mod error;
mod models;
mod persistence;
mod scheduler;
mod telemetry;

use admission::limiter::TenantLimiter;
use admission::queue::AdmissionQueue;
use config::AppConfig;
use edge::router::{create_router, AppState};
use edge::streaming::StreamEngine;
use models::BackendRegistration;
use persistence::store::PersistenceStore;
use scheduler::health::HealthProber;
use scheduler::registry::BackendRegistry;
use scheduler::strategy::Scheduler;
use std::sync::Arc;
use telemetry::metrics::MetricsState;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "inference_os=info,tower_http=info".into()),
        )
        .init();

    info!("Starting InferenceOS: Adaptive Model Serving Gateway & Control Plane");

    let config = AppConfig::from_env();
    info!("Configuration: host={}:{}, allow_local={}", config.host, config.port, config.allow_local_backends);

    let registry = BackendRegistry::new(config.allow_local_backends);

    // Bootstrap default local backends for local-first testing
    if config.allow_local_backends {
        let _ = registry.register_backend(BackendRegistration {
            name: "worker-fast".to_string(),
            url: "http://127.0.0.1:8001".to_string(),
            model_ids: vec!["llama3".to_string(), "qwen2.5".to_string(), "mistral".to_string(), "*".to_string()],
            concurrency_limit: 10,
            weight: 2,
        });

        let _ = registry.register_backend(BackendRegistration {
            name: "worker-slow".to_string(),
            url: "http://127.0.0.1:8002".to_string(),
            model_ids: vec!["llama3".to_string(), "qwen2.5".to_string(), "mistral".to_string(), "*".to_string()],
            concurrency_limit: 5,
            weight: 1,
        });

        let _ = registry.register_backend(BackendRegistration {
            name: "worker-chaos".to_string(),
            url: "http://127.0.0.1:8003".to_string(),
            model_ids: vec!["chaos-model".to_string()],
            concurrency_limit: 5,
            weight: 1,
        });
        info!("Bootstrapped default local worker backends (ports 8001, 8002, 8003)");
    }

    let scheduler = Scheduler::new(registry.clone());
    let limiter = TenantLimiter::new(config.default_tenant_token_budget, config.default_tenant_max_inflight);
    let queue = AdmissionQueue::new(config.max_queue_depth, config.queue_timeout_ms);
    let metrics = Arc::new(MetricsState::new());
    let stream_engine = Arc::new(StreamEngine::new(registry.clone(), limiter.clone(), metrics.clone()));
    let persistence = Arc::new(PersistenceStore::new());

    // Start background health prober
    let health_prober = Arc::new(HealthProber::new(registry.clone(), config.health_check_interval_secs));
    health_prober.start();
    info!("Active background health prober initialized");

    let state = AppState {
        config: config.clone(),
        registry,
        scheduler,
        limiter,
        queue,
        stream_engine,
        persistence,
        metrics,
    };

    let app = create_router(state);

    let bind_addr = format!("{}:{}", config.host, config.port);
    let listener = tokio::net::TcpListener::bind(&bind_addr).await?;
    info!("InferenceOS Gateway listening on http://{}", bind_addr);
    info!("OpenAI Chat Endpoint: http://{}/v1/chat/completions", bind_addr);
    info!("Admin Backends:       http://{}/admin/backends", bind_addr);
    info!("Prometheus Metrics:   http://{}/metrics", bind_addr);

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    info!("InferenceOS Gateway shut down cleanly");
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {
            info!("Received Ctrl+C, initiating graceful drain and shutdown...");
        },
        _ = terminate => {
            info!("Received SIGTERM, initiating graceful drain and shutdown...");
        },
    }
}
