# InferenceOS: Adaptive Model Serving Gateway & Inference Control Plane

[![Rust](https://img.shields.io/badge/Rust-1.80+-orange.svg)](https://www.rust-lang.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Tests: 13 Passed](https://img.shields.io/badge/Tests-13%20Passed-brightgreen.svg)]()
[![OpenAI Compatible](https://img.shields.io/badge/API-OpenAI%20Compatible-green.svg)]()

> **One OpenAI-compatible endpoint that routes requests to the right local model server with predictable latency, fairness, and failure recovery.**

---

## 1. Overview & Core Thesis

In modern AI platforms, serving heterogeneous model servers (`llama.cpp`, `Ollama`, `vLLM`, or dedicated GPU nodes) presents critical distributed systems challenges:
- Traffic is spiky; one model server gets saturated while another is idle.
- Long reasoning prompts block short interactive requests.
- Worker crashes mid-stream risk duplicate text generation or corrupted state.

**InferenceOS** is a self-hosted high-concurrency gateway built in **Rust (Tokio + Axum)**. It accepts standard OpenAI chat completion requests (`/v1/chat/completions`), estimates token demand, enforces tenant token budgets, and routes requests to the healthiest worker using load-aware scheduling algorithms.

```
                              [ Client SDK / Web UI ]
                                         │
                                         ▼
                     ┌───────────────────────────────────────┐
                     │          InferenceOS Gateway          │
                     │  - Axum HTTP & SSE Streaming Engine   │
                     │  - Token Budget Admission Controller  │
                     │  - Cancellation & Safe Retry Pipeline │
                     └───────────────────┬───────────────────┘
                                         │
                     ┌───────────────────┴───────────────────┐
                     ▼                                       ▼
           [ Fast Model Worker ]                   [ Slow Model Worker ]
         (e.g., GPU vLLM / Stub)                 (e.g., CPU llama.cpp)
           TTFT: 20ms | 6ms/tok                    TTFT: 75ms | 18ms/tok
```

---

## 2. System Architecture & Core Invariants

### Architectural Decision Records (ADRs)

1. **[ADR-001: Irreversible Streaming Commitment (Safe Retry)](docs/adr/001-safe-retry-contract.md)**:
   - **Pre-Stream Retry**: If an upstream worker fails *before* any HTTP headers or the first SSE token chunk is flushed to the client, the gateway safely fails over to a secondary healthy backend.
   - **Post-Stream Abort**: Once the first token is flushed, the request is **committed**. Mid-stream crashes terminate cleanly with an error frame and **never** auto-retry across backends, preventing duplicate or corrupted output.
2. **[ADR-002: Server-Side Request Forgery (SSRF) Defense](docs/adr/002-ssrf-defense.md)**:
   - Dynamic worker registration (`POST /admin/backends`) strictly blocks cloud instance metadata endpoints (`169.254.169.254`) and restricted private CIDRs.
3. **[ADR-003: Two-Phase Token Budget Accounting](docs/adr/003-token-budget-accounting.md)**:
   - Evaluates a soft token reservation before admission to prevent cluster starvation.
   - Reconciles with actual model-reported usage upon completion.

---

## 3. Empirical Benchmark Results

We evaluated **Round-Robin** vs **Queue-Aware (Estimated Queue Delay)** scheduling under high concurrency (30 requests across 6 concurrent workers with mixed short/long prompt distributions):

| Metric | Round-Robin (Baseline) | Queue-Aware (EQD) | Measured Improvement |
|---|---|---|---|
| **p50 TTFT (ms)** | `79.43 ms` | **`41.89 ms`** | **-47.2% latency** |
| **p90 TTFT (ms)** | `100.90 ms` | **`84.43 ms`** | **-16.3% latency** |
| **p95 TTFT (ms)** | `101.41 ms` | **`87.42 ms`** | **-13.8% latency** |
| **Throughput (tokens/sec)** | `283.5 tok/s` | **`413.5 tok/s`** | **+45.8% throughput** |
| **Throughput (reqs/min)** | `1063.3 req/m` | **`1550.8 req/m`** | **+45.8% throughput** |
| **Worker Dispatch Ratio** | `50% Fast / 50% Slow` | **`90% Fast / 10% Slow`** | Dynamic work-stealing |

*Raw reproducible benchmark data archived in [`benchmarks/results/benchmark_summary.json`](benchmarks/results/benchmark_summary.json).*

---

## 4. Key Endpoints

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/v1/chat/completions` | OpenAI-compatible chat completions (supports SSE streaming & non-streaming) |
| `GET` | `/admin/backends` | Inspect all registered workers, states, and active queue depths |
| `POST` | `/admin/backends` | Register dynamic model endpoint (with SSRF defense) |
| `POST` | `/admin/backends/:id/drain` | Graceful zero-downtime worker draining |
| `GET` | `/admin/tenants` | Multi-tenant token budgets and inflight request metrics |
| `POST` | `/admin/tenants` | Provision or update tenant quotas |
| `GET` | `/admin/routes/audit` | Transparent scheduling audit trail with candidate scoring |
| `GET` | `/metrics` | Prometheus metrics exposition (`TTFT`, total latency, cancellations) |
| `GET` | `/healthz` & `/readyz` | Kubernetes-compatible liveness and readiness probes |

---

## 5. Quickstart & Local-First Verification

### Prerequisites
- Rust 1.80+ (or Cargo)
- Python 3.11+

### 1. Run Unit, Integration & Chaos Tests (13 Passing)
```bash
python -m pytest tests/ -v
```
Output:
```
tests/test_chaos_invariants.py::test_mid_stream_worker_crash_terminates_cleanly_without_duplicate PASSED
tests/test_chaos_invariants.py::test_ten_tenants_isolation_and_fairness PASSED
tests/test_chaos_invariants.py::test_queue_overload_shedding PASSED
tests/test_gateway_e2e.py::test_healthz_and_readyz PASSED
tests/test_gateway_e2e.py::test_metrics_prometheus_exposition PASSED
tests/test_gateway_e2e.py::test_admin_backends_and_ssrf_rejection PASSED
tests/test_gateway_e2e.py::test_admin_draining_mode PASSED
tests/test_gateway_e2e.py::test_chat_completion_non_streaming PASSED
tests/test_gateway_e2e.py::test_chat_completion_streaming_sse PASSED
tests/test_gateway_e2e.py::test_client_cancellation_recorded PASSED
tests/test_gateway_e2e.py::test_adr001_safe_retry_pre_stream_failure PASSED
tests/test_gateway_e2e.py::test_tenant_authorization_and_budget_limits PASSED
tests/test_gateway_e2e.py::test_multi_strategy_routing_and_audit PASSED

============================= 13 passed in 9.86s =============================
```

### 2. Run the Empirical Benchmark Suite
```bash
python benchmarks/compare_schedulers.py
```

### 3. Native Run
Start the deterministic fast/slow model stubs:
```bash
python stubs/run_cluster.py
```
In another terminal, start the InferenceOS gateway:
```bash
cargo run
```

### 4. Docker Compose Deployment ($0 Cost)
```bash
docker compose --profile cpu-demo up --build
```
- Gateway: `http://localhost:8080`
- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000` (admin/admin)

---

## 6. Recruiter & Live Demo Walkthrough

1. **Baseline Load Distribution**:
   - Query `/v1/chat/completions` using header `x-inferenceos-policy: round-robin`. Observe cyclic dispatch between `worker-fast` and `worker-slow`.
2. **Queue-Aware Optimization**:
   - Query using header `x-inferenceos-policy: estimated-queue-delay`. Observe automatic steering to `worker-fast`, avoiding slow queue buildup.
3. **Fault Injection & Chaos**:
   - Trigger a pre-stream 500 error (`x-chaos-fail-pre-stream: true`). Observe transparent failover to secondary worker with zero user-facing error.
   - Trigger a mid-stream disconnect (`x-chaos-fail-mid-stream: true`). Verify stream terminates cleanly with zero duplicate token replay.
4. **Zero-Downtime Maintenance**:
   - Execute `POST /admin/backends/backend-1/drain`. Watch all new traffic route to alternate workers without dropping inflight requests.

---

## 7. Resume Headline & Bullet

**Headline**: InferenceOS — Adaptive Rust Model Serving Gateway & Inference Control Plane

**Resume Bullet**:
> *Architected an OpenAI-compatible inference serving gateway in Rust (Tokio/Axum) with SSE streaming, bounded admission queues, and multi-tenant token budgets; implemented Estimated-Queue-Delay (EQD) scheduling that reduced p95 TTFT by 13.8% and increased system throughput by 45.8% across heterogeneous local model backends.*
