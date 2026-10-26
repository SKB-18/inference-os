use crate::error::GatewayError;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[derive(Debug, Clone)]
pub struct AdmissionQueue {
    semaphore: Arc<Semaphore>,
    max_depth: usize,
    timeout: Duration,
}

impl AdmissionQueue {
    pub fn new(max_depth: usize, timeout_ms: u64) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(max_depth)),
            max_depth,
            timeout: Duration::from_millis(timeout_ms),
        }
    }

    pub fn current_waiting_count(&self) -> usize {
        let available = self.semaphore.available_permits();
        self.max_depth.saturating_sub(available)
    }

    pub async fn admit(&self) -> Result<OwnedSemaphorePermit, GatewayError> {
        let available = self.semaphore.available_permits();
        if available == 0 {
            return Err(GatewayError::QueueOverloaded {
                current_depth: self.max_depth,
                max_depth: self.max_depth,
            });
        }

        match tokio::time::timeout(self.timeout, self.semaphore.clone().acquire_owned()).await {
            Ok(Ok(permit)) => Ok(permit),
            Ok(Err(_)) => Err(GatewayError::Internal("Admission queue semaphore closed".to_string())),
            Err(_) => Err(GatewayError::QueueTimeout {
                waited_ms: self.timeout.as_millis() as u64,
            }),
        }
    }
}
