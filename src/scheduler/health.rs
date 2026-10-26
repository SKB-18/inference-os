use crate::scheduler::registry::BackendRegistry;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, warn};

pub struct HealthProber {
    registry: BackendRegistry,
    client: reqwest::Client,
    interval: Duration,
}

impl HealthProber {
    pub fn new(registry: BackendRegistry, interval_secs: u64) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(1500))
            .build()
            .unwrap_or_default();

        Self {
            registry,
            client,
            interval: Duration::from_secs(interval_secs.max(1)),
        }
    }

    pub fn start(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(self.interval);
            loop {
                interval.tick().await;
                self.probe_all().await;
            }
        })
    }

    pub async fn probe_all(&self) {
        let backends = self.registry.list_backends();
        for b in backends {
            let probe_url = format!("{}/healthz", b.url.trim_end_matches('/'));
            match self.client.get(&probe_url).send().await {
                Ok(resp) if resp.status().is_success() => {
                    debug!("Health probe passed for backend '{}' at {}", b.id, probe_url);
                    let _ = self.registry.release_inflight(&b.id, true, None);
                }
                Ok(resp) => {
                    warn!("Health probe returned non-200 ({}) for backend '{}' at {}", resp.status(), b.id, probe_url);
                    let _ = self.registry.release_inflight(&b.id, false, None);
                }
                Err(e) => {
                    warn!("Health probe failed ({}) for backend '{}' at {}", e, b.id, probe_url);
                    let _ = self.registry.release_inflight(&b.id, false, None);
                }
            }
        }
    }
}
