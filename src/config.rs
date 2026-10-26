use std::env;

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub host: String,
    pub port: u16,
    pub admin_api_key: String,
    pub max_queue_depth: usize,
    pub queue_timeout_ms: u64,
    pub default_upstream_timeout_ms: u64,
    pub health_check_interval_secs: u64,
    pub allow_local_backends: bool,
    pub database_url: Option<String>,
    pub default_tenant_token_budget: u64,
    pub default_tenant_max_inflight: usize,
    pub max_retries: usize,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".to_string(),
            port: 8080,
            admin_api_key: "admin-secret-key-123".to_string(),
            max_queue_depth: 500,
            queue_timeout_ms: 15_000,
            default_upstream_timeout_ms: 60_000,
            health_check_interval_secs: 5,
            allow_local_backends: true,
            database_url: None,
            default_tenant_token_budget: 1_000_000,
            default_tenant_max_inflight: 25,
            max_retries: 1,
        }
    }
}

impl AppConfig {
    pub fn from_env() -> Self {
        let _ = dotenvy::dotenv();

        let mut config = Self::default();

        if let Ok(val) = env::var("INFERENCEOS_HOST") {
            config.host = val;
        }
        if let Ok(val) = env::var("INFERENCEOS_PORT") {
            if let Ok(p) = val.parse::<u16>() {
                config.port = p;
            }
        }
        if let Ok(val) = env::var("INFERENCEOS_ADMIN_KEY") {
            config.admin_api_key = val;
        }
        if let Ok(val) = env::var("INFERENCEOS_MAX_QUEUE_DEPTH") {
            if let Ok(d) = val.parse::<usize>() {
                config.max_queue_depth = d;
            }
        }
        if let Ok(val) = env::var("INFERENCEOS_QUEUE_TIMEOUT_MS") {
            if let Ok(t) = val.parse::<u64>() {
                config.queue_timeout_ms = t;
            }
        }
        if let Ok(val) = env::var("INFERENCEOS_UPSTREAM_TIMEOUT_MS") {
            if let Ok(t) = val.parse::<u64>() {
                config.default_upstream_timeout_ms = t;
            }
        }
        if let Ok(val) = env::var("INFERENCEOS_HEALTH_CHECK_INTERVAL_SECS") {
            if let Ok(s) = val.parse::<u64>() {
                config.health_check_interval_secs = s;
            }
        }
        if let Ok(val) = env::var("INFERENCEOS_ALLOW_LOCAL_BACKENDS") {
            config.allow_local_backends = val.to_lowercase() == "true" || val == "1";
        }
        if let Ok(val) = env::var("DATABASE_URL") {
            if !val.trim().is_empty() {
                config.database_url = Some(val);
            }
        }

        config
    }
}
