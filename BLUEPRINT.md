# InferenceOS: Adaptive Model Serving Gateway and Inference Control Plane

> **Blueprint 01 | Future-Ready Software Engineering | Free / Open-Source First**
> *One OpenAI-compatible endpoint that routes requests to the right local model server with predictable latency, fairness, and failure recovery.*

---

## 00. Project Overview & Quick Reference

| Attribute | Specification |
|---|---|
| **Primary Skills** | Rust / Async Systems (Tokio, Axum), API Design, Scheduling, AI Infrastructure |
| **Cost Category** | $0 subscription; GPU-scale benchmark is hardware-dependent |
| **Build Horizon** | 10–14 weeks focused |
| **Hardware Profile** | CPU mode: 16 GB preferred; realistic high-throughput GPU optional |
| **Target Resume Area**| AI Infrastructure / Backend / Systems Performance |

---

## 01. The Complete Idea

A SaaS engineering team uses several models for summarization, extraction, and coding assistance. Traffic is spiky; one model server gets saturated, another is idle, and long requests block short ones. 

**InferenceOS** is a self-hosted gateway that accepts a standard chat-completions request (`/v1/chat/completions`), estimates demand, checks model capability and tenant budget, and chooses a healthy backend. The user sees one unified API even when the platform runs a mixture of local `llama.cpp` servers, `Ollama` instances, or compatible `vLLM` workers.

> **Key Distinction**: This is **not** a model-training project and **not** another chat UI. The core engineering contribution is the **control plane**: measured queueing policies, cancellation propagation, streaming propagation, resource-aware routing, and failover without duplicate charging. It should expose *why* each scheduling decision was made.

---

## 02. Target Users & Measurable Outcomes

- **Platform Engineer**: Deploys new backends and sets per-tenant fairness weights.
- **Application Developer**: Streams responses without needing to know which backend model server serves them.
- **SRE (Site Reliability Engineer)**: Traces errors, queue delay, backpressure, capacity, and service-level objectives (SLOs).

---

## 03. Concrete Product Workflow

1. **Register Backends**: Register two local OpenAI-compatible model endpoints with distinct max concurrency and capabilities.
2. **Client Dispatch**: Client sends a chat request with model class, latency tier, max tokens, and an idempotency key.
3. **Admission Control**: Gateway authenticates client, validates tokens/budget, and admits request into a bounded queue.
4. **Scheduling & Execution**: Scheduler estimates cost and queue wait, picks a healthy worker, and forwards a streaming or non-streaming request.
5. **Fault Recovery**: On worker timeout or failure, retry only when replay is safe (never auto-retry after streaming has started!); return structured errors otherwise.
6. **Observability**: Metrics dashboard displays latency (p50/p95/p99), tokens/sec, queue time, budget usage, and routing rationales.

---

## 04. Architectural Blueprint

```
Client SDK -> API Gateway (Edge) -> Admission / Auth / Budgets
                                         |
                                         v
                                  Scheduler + Queue
                                       /   \
                         llama.cpp / Ollama  vLLM (optional)
                                       \   /
                            OTel + Prometheus + PostgreSQL
```

### Components & Responsibilities

| Component | Responsibilities & Engineering Detail |
|---|---|
| **API Edge** | Axum HTTP/SSE; request validation, auth, cancellation forwarding, idempotency and correlation IDs. |
| **Admission Controller** | Per-tenant rate limits and token budgets; bounded queues, 429/503 overload responses, priority starvation limits. |
| **Router / Scheduler** | Baseline round-robin; least-inflight and estimated-queue-delay algorithms; configurable health probes. |
| **Backend Adapter** | Normalize OpenAI-compatible streaming semantics, finish reasons, usage accounting, and timeouts. |
| **Telemetry Plane** | OpenTelemetry traces; Prometheus histograms for TTFT (Time-To-First-Token), total latency, error classes, and cost estimates. |
| **Control API** | Register backends, mark draining, set routing weights, inspect service state; guarded by admin auth. |

---

## 05. Features: MVP, v1, and Advanced

```
+---------------------------------------------------------------------------------+
| MVP (Usable)                                                                    |
| - Single gateway                                                                |
| - Two local backends                                                            |
| - Round-robin routing                                                           |
| - SSE streaming                                                                 |
| - Health checks                                                                 |
| - Prometheus metrics                                                            |
+---------------------------------------------------------------------------------+
                                      |
                                      v
+---------------------------------------------------------------------------------+
| v1 (Reliable)                                                                   |
| - Load-aware router (least-inflight & estimated-queue-delay)                    |
| - Bounded queue with backpressure (429/503)                                     |
| - Token budgets & tenant isolation                                              |
| - Retries with safety rules (no retry once stream started)                      |
| - Circuit breaker                                                               |
| - Observability dashboard                                                       |
+---------------------------------------------------------------------------------+
                                      |
                                      v
+---------------------------------------------------------------------------------+
| Advanced (Optional Extensions)                                                  |
| - Dynamic model capability ranking                                              |
| - Canary rollout of backends                                                    |
| - Batch-aware scheduling                                                        |
| - Prefix-cache locality hints                                                   |
| - Autoscaling simulation                                                        |
+---------------------------------------------------------------------------------+
```

---

## 06. Free Stack Choices ($0 Cost)

| Layer | Zero-Subscription Solution | Purpose / Fallback |
|---|---|---|
| **API Gateway** | Rust + Tokio + Axum *(or Python Async/FastAPI fallback)* | High-concurrency gateway; HTTP streams + cancellation. |
| **Model Servers** | `llama.cpp` server / `Ollama` | Local small models; zero external paid token API. |
| **GPU Option** | `vLLM` self-hosted | Use only if compatible local GPU is present; CPU stubs are valid. |
| **Database** | PostgreSQL | Persistent tenants, backend configs, budgets, audit records. |
| **Observability** | OpenTelemetry + Prometheus + Grafana | Self-hosted locally; zero hosted monitoring fees. |
| **Packaging** | Docker Compose / Podman Compose | Run gateway + database + two fake/real model backends. |
| **Testing** | k6 / Locust + Criterion / Pytest | Synthetic OpenAI-compatible backend and load tests. |

---

## 07. Key Contracts & Persistence Model

### External Interfaces
- `POST /v1/chat/completions`: Accepts `ChatCompletionRequest` -> returns SSE token stream (`text/event-stream`) or JSON response.
- `GET /admin/backends`: Lists `backend_id`, `status`, `queue_depth`, `concurrency_limit`, `capabilities`.
- `POST /admin/backends`: Registers endpoint URL and supported model IDs; enforces SSRF defense (restricts destination hosts/private CIDRs).
- `GET /metrics`: Prometheus scrape endpoint.
- `GET /healthz` & `GET /readyz`: Liveness and readiness probes.

### Minimum Durable Entities (PostgreSQL)
- **`backend`**: `id`, `url`, `model_ids`, `concurrency_limit`, `weight`, `state`, `updated_at`
- **`tenant`**: `id`, `token_budget`, `max_inflight`, `allowed_models`, `policy_version`
- **`request`**: `id`, `tenant_id`, `idempotency_key`, `selected_backend`, `state`, `latency_ms`, `tokens_estimated`, `tokens_used`
- **`route_decision`**: `request_id`, `candidates`, `exclusion_reasons`, `policy`, `selected_at`

---

## 08. Local-First Deployment Strategy ($0 Subscriptions)

1. Run two deterministic stub-model servers: each emits fake tokens with configurable wait/fail/drop rates.
2. Bring up gateway with Docker Compose profile `cpu-demo`; load configuration from `.env.example`.
3. Swap one stub for a small quantized GGUF model via `llama.cpp` or `Ollama` and verify streaming end-to-end.
4. Run repeatable workload scripts; archive anonymized aggregate metrics and benchmark environment info.
5. *(Optional)* Self-host `vLLM` if compatible GPU exists.

---

## 09. Step-by-Step Engineering Roadmap

1. **Phase 1 - Spec**: Define API/streaming protocol, failure semantics, routing invariants, workload generator, contract tests, and ADR for retry/idempotency policy.
2. **Phase 2 - Edge**: Implement gateway endpoints, SSE piping, auth middleware, graceful shutdown, and cancellation tests.
3. **Phase 3 - Routing**: Implement backend registry, round-robin, least-inflight, estimated-queue-delay, bounded queues, and overload handling.
4. **Phase 4 - Persistence**: Store tenant limits, configuration changes, request audit summaries, and schema migrations.
5. **Phase 5 - Resilience**: Implement timeouts, health probes, circuit breaking, safe retry contracts, and fault injection suite.
6. **Phase 6 - Bench**: Build load generator with prompt length distributions; compare schedulers (p50/p95/p99 charts).
7. **Phase 7 - Ops**: Grafana dashboards, alerts, Docker Compose manifests, CI, and runbooks.

---

## 10. Production-Grade Verification & Failure Testing

- **Streaming**: Chunk ordering, client disconnect / cancellation forwarding, unexpected worker EOF, SSE parse errors, late usage frame.
- **Resilience**: Worker crash mid-stream, transient network partition, 500 error storms, warm-to-cold backend transitions.
- **Fairness**: 10 tenants with unequal loads; assert bounded starvation and budget enforcement.
- **Security**: Bearer token auth, SSRF protection on admin backend registration, output redaction, non-leaking logs.
- **Correctness**: Tokens billed only once, idempotency key under retries, no routing to draining backends.

---

## 11. Benchmark Plan: What to Measure & Prove

| Metric | Baseline & Measurement Method | Target / Success Criterion |
|---|---|---|
| **TTFT p95** | Same request mix, identical two backends; compare round-robin vs queue-aware. | Measurable % reduction in p95 Time-To-First-Token. |
| **Throughput** | Complete requests/min and tokens/sec at 1, 5, 20 concurrent clients. | Stable plateau before overload; graceful degradation. |
| **Errors Under Failure** | Kill one worker at 50% load; count failed vs correctly recovered requests. | Zero stuck requests; defined retry/fail outcome. |
| **Fairness** | Share of served work and p95 queue wait by tenant under uneven load. | No unbounded tenant starvation. |

---

## 12. Security, Compliance, & Safety Decisions

- **Zero Content Leakage**: Never log prompt content by default; use anonymized request IDs, redacted metadata, and sampling.
- **SSRF Defense**: Strict allowlist on backend registration to prevent SSRF targeting cloud metadata (`169.254.169.254`) or unauthorized internal services.
- **Token Accounting**: Explicitly separate soft token estimates (heuristic token count) from actual model-reported usage.
- **Safe Retries**: **Never** auto-retry after streaming has started (to prevent duplicated or corrupted output).

---

## 13. Deliverables & Resume Impact

- **Resume Headline**: `InferenceOS - adaptive model serving gateway and inference control plane`
- **Resume Bullet**: *Built a high-concurrency model gateway with token streaming, bounded admission, tenant budgets, and fault-aware routing; improved p95 TTFT by [X]% versus baseline under high-concurrency workloads.*
