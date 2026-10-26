use crate::admission::limiter::TenantLimiter;
use crate::error::GatewayError;
use crate::models::{Backend, ChatCompletionChunk, ChatCompletionRequest, ChatCompletionResponse, Tenant};
use crate::scheduler::registry::BackendRegistry;
use crate::telemetry::metrics::MetricsState;
use axum::response::sse::{Event, KeepAlive, Sse};
use futures::stream::Stream;
use futures_util::StreamExt;
use std::convert::Infallible;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{error, warn};

pub struct StreamEngine {
    client: reqwest::Client,
    registry: BackendRegistry,
    limiter: TenantLimiter,
    metrics: Arc<MetricsState>,
}

impl StreamEngine {
    pub fn new(
        registry: BackendRegistry,
        limiter: TenantLimiter,
        metrics: Arc<MetricsState>,
    ) -> Self {
        let client = reqwest::Client::builder()
            .pool_idle_timeout(std::time::Duration::from_secs(90))
            .pool_max_idle_per_host(20)
            .build()
            .unwrap_or_default();

        Self {
            client,
            registry,
            limiter,
            metrics,
        }
    }

    /// Executes non-streaming request
    pub async fn execute_non_streaming(
        &self,
        backend: &Backend,
        tenant: &Tenant,
        req: &ChatCompletionRequest,
        estimated_tokens: u32,
        extra_headers: Option<&axum::http::HeaderMap>,
    ) -> Result<ChatCompletionResponse, GatewayError> {
        let start = Instant::now();
        let target_url = format!("{}/v1/chat/completions", backend.url.trim_end_matches('/'));

        let mut req_builder = self.client.post(&target_url).json(req);
        if let Some(hdrs) = extra_headers {
            for (k, v) in hdrs {
                if k.as_str().starts_with("x-chaos-") {
                    req_builder = req_builder.header(k.as_str(), v);
                }
            }
        }

        let resp = req_builder
            .send()
            .await
            .map_err(|e| GatewayError::UpstreamError(format!("Request to {} failed: {}", target_url, e)))?;

        let status = resp.status();
        if !status.is_success() {
            let err_text = resp.text().await.unwrap_or_else(|_| "Unknown upstream error".to_string());
            return Err(GatewayError::UpstreamError(format!("Upstream returned {}: {}", status, err_text)));
        }

        let parsed: ChatCompletionResponse = resp.json().await
            .map_err(|e| GatewayError::UpstreamError(format!("Failed to parse completion JSON: {}", e)))?;

        let latency_ms = start.elapsed().as_secs_f64() * 1000.0;
        let actual_tokens = parsed.usage.as_ref().map(|u| u.total_tokens).unwrap_or(estimated_tokens);

        self.limiter.settle_budget(&tenant.id, estimated_tokens, actual_tokens);
        self.registry.release_inflight(&backend.id, true, Some(latency_ms));
        self.metrics.record_latency(latency_ms);
        self.metrics.record_ttft(latency_ms);
        self.metrics.successful_requests.fetch_add(1, Ordering::Relaxed);
        self.metrics.record_backend_served(&backend.id);

        Ok(parsed)
    }

    /// Executes streaming request with SSE piping, cancellation propagation, and safe retry detection
    pub async fn execute_streaming(
        &self,
        backend: &Backend,
        tenant: &Tenant,
        req: &ChatCompletionRequest,
        estimated_tokens: u32,
        extra_headers: Option<&axum::http::HeaderMap>,
    ) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, GatewayError> {
        let start = Instant::now();
        let target_url = format!("{}/v1/chat/completions", backend.url.trim_end_matches('/'));

        let mut req_builder = self.client.post(&target_url).json(req);
        if let Some(hdrs) = extra_headers {
            for (k, v) in hdrs {
                if k.as_str().starts_with("x-chaos-") {
                    req_builder = req_builder.header(k.as_str(), v);
                }
            }
        }

        // 1. Initial connection to worker (pre-stream phase)
        let resp = req_builder
            .send()
            .await
            .map_err(|e| GatewayError::UpstreamError(format!("Connect to {} failed: {}", target_url, e)))?;

        let status = resp.status();
        if !status.is_success() {
            let err_text = resp.text().await.unwrap_or_else(|_| "Unknown upstream error".to_string());
            return Err(GatewayError::UpstreamError(format!("Upstream returned {}: {}", status, err_text)));
        }

        let mut byte_stream = resp.bytes_stream();
        let (tx, rx) = mpsc::channel::<Result<Event, Infallible>>(64);

        let backend_id = backend.id.clone();
        let tenant_id = tenant.id.clone();
        let registry = self.registry.clone();
        let limiter = self.limiter.clone();
        let metrics = self.metrics.clone();

        // Spawn stream forwarder
        tokio::spawn(async move {
            let mut first_token_flushed = false;
            let mut prompt_tokens_used = 15;
            let mut completion_tokens_count = 0u32;
            let mut buffer = String::new();

            loop {
                tokio::select! {
                    // Check if receiver closed (client disconnect / cancellation)
                    _ = tx.closed() => {
                        metrics.client_cancellations.fetch_add(1, Ordering::Relaxed);
                        warn!("Client disconnected mid-stream from backend '{}'", backend_id);
                        break;
                    }
                    chunk = byte_stream.next() => {
                        match chunk {
                            Some(Ok(bytes)) => {
                                let chunk_str = String::from_utf8_lossy(&bytes);
                                buffer.push_str(&chunk_str);

                                // Process full SSE lines from buffer
                                while let Some(pos) = buffer.find("\n\n") {
                                    let full_msg = buffer[..pos].to_string();
                                    buffer = buffer[pos + 2..].to_string();

                                    for line in full_msg.lines() {
                                        if let Some(data_str) = line.strip_prefix("data: ") {
                                            if data_str.trim() == "[DONE]" {
                                                let _ = tx.send(Ok(Event::default().data("[DONE]"))).await;
                                                continue;
                                            }

                                            // Count completion token
                                            completion_tokens_count += 1;

                                            if !first_token_flushed {
                                                first_token_flushed = true;
                                                let ttft_ms = start.elapsed().as_secs_f64() * 1000.0;
                                                metrics.record_ttft(ttft_ms);
                                                metrics.record_backend_served(&backend_id);
                                            }

                                            // Try to extract usage block if reported
                                            if let Ok(parsed_chunk) = serde_json::from_str::<ChatCompletionChunk>(data_str) {
                                                if let Some(u) = parsed_chunk.usage {
                                                    prompt_tokens_used = u.prompt_tokens;
                                                    completion_tokens_count = u.completion_tokens;
                                                }
                                            }

                                            let event = Event::default().data(data_str);
                                            if tx.send(Ok(event)).await.is_err() {
                                                // Downstream disconnected
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                            Some(Err(err)) => {
                                error!("Upstream stream read error from backend '{}': {}", backend_id, err);
                                // ADR-001: Once first token is sent, DO NOT RETRY. Terminate with error.
                                let err_event = Event::default()
                                    .data(r#"{"error":{"message":"Upstream worker crashed mid-stream","type":"upstream_error"}}"#);
                                let _ = tx.send(Ok(err_event)).await;
                                break;
                            }
                            None => {
                                // Clean stream EOF
                                break;
                            }
                        }
                    }
                }
            }

            let total_latency_ms = start.elapsed().as_secs_f64() * 1000.0;
            metrics.record_latency(total_latency_ms);

            let actual_tokens = prompt_tokens_used + completion_tokens_count;
            limiter.settle_budget(&tenant_id, estimated_tokens, actual_tokens);
            registry.release_inflight(&backend_id, true, Some(total_latency_ms));
            metrics.successful_requests.fetch_add(1, Ordering::Relaxed);
            metrics.total_prompt_tokens.fetch_add(prompt_tokens_used as u64, Ordering::Relaxed);
            metrics.total_completion_tokens.fetch_add(completion_tokens_count as u64, Ordering::Relaxed);
        });

        let sse_stream = ReceiverStream::new(rx);
        Ok(Sse::new(sse_stream).keep_alive(KeepAlive::default()))
    }
}
