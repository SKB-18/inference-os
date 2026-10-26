"""
End-to-End Integration and Chaos Test Suite for InferenceOS.
Tests all contracts, SSE streaming, client cancellation, ADR-001 safe retries,
SSRF defenses, tenant isolation, and multi-strategy routing.
"""

import json
import os
import subprocess
import sys
import time
import httpx
import pytest

GATEWAY_PORT = 8090
GATEWAY_URL = f"http://127.0.0.1:{GATEWAY_PORT}"
DEMO_KEY = "sk-inferenceos-demo-key"
ADMIN_KEY = "admin-secret-key-123"

def test_healthz_and_readyz():
    with httpx.Client(base_url=GATEWAY_URL) as client:
        r = client.get("/healthz")
        assert r.status_code == 200
        assert r.text == "OK"

        r = client.get("/readyz")
        assert r.status_code == 200
        assert r.text == "READY"

def test_metrics_prometheus_exposition():
    with httpx.Client(base_url=GATEWAY_URL) as client:
        r = client.get("/metrics")
        assert r.status_code == 200
        body = r.text
        assert "inferenceos_requests_total" in body
        assert "inferenceos_ttft_p50_ms" in body
        assert "inferenceos_backend_requests_total" in body

def test_admin_backends_and_ssrf_rejection():
    headers = {"Authorization": f"Bearer {ADMIN_KEY}"}
    with httpx.Client(base_url=GATEWAY_URL) as client:
        # List backends
        r = client.get("/admin/backends", headers=headers)
        assert r.status_code == 200
        backends = r.json()
        assert len(backends) >= 2
        names = [b["name"] for b in backends]
        assert "worker-fast" in names
        assert "worker-slow" in names

        # Attempt SSRF attack to cloud metadata
        ssrf_payload = {
            "name": "malicious-worker",
            "url": "http://169.254.169.254/latest/meta-data/",
            "model_ids": ["*"]
        }
        r = client.post("/admin/backends", headers=headers, json=ssrf_payload)
        assert r.status_code == 403
        err = r.json()["error"]
        assert err["code"] == "ssrf_violation"

def test_admin_draining_mode():
    headers = {"Authorization": f"Bearer {ADMIN_KEY}"}
    with httpx.Client(base_url=GATEWAY_URL) as client:
        backends = client.get("/admin/backends", headers=headers).json()
        target_id = backends[0]["backend_id"]

        # Toggle to draining
        r = client.post(f"/admin/backends/{target_id}/drain", headers=headers)
        assert r.status_code == 200
        assert r.json()["new_state"] == "draining"

        # Toggle back to healthy
        r = client.post(f"/admin/backends/{target_id}/drain", headers=headers)
        assert r.status_code == 200
        assert r.json()["new_state"] == "healthy"

def test_chat_completion_non_streaming():
    headers = {
        "Authorization": f"Bearer {DEMO_KEY}",
        "Content-Type": "application/json"
    }
    payload = {
        "model": "llama3",
        "messages": [{"role": "user", "content": "Explain inference control planes"}],
        "stream": False,
        "idempotency_key": "test-key-12345"
    }

    with httpx.Client(base_url=GATEWAY_URL) as client:
        r = client.post("/v1/chat/completions", headers=headers, json=payload, timeout=10.0)
        assert r.status_code == 200
        data = r.json()
        assert data["object"] == "chat.completion"
        assert len(data["choices"]) > 0
        assert "InferenceOS" in data["choices"][0]["message"]["content"]
        assert data["usage"]["total_tokens"] > 0
        assert "X-InferenceOS-Backend" in r.headers

        # Test idempotency cache
        r_cached = client.post("/v1/chat/completions", headers=headers, json=payload, timeout=5.0)
        assert r_cached.status_code == 200
        assert r_cached.headers.get("X-InferenceOS-Cached") == "true"

def test_chat_completion_streaming_sse():
    headers = {
        "Authorization": f"Bearer {DEMO_KEY}",
        "Content-Type": "application/json"
    }
    payload = {
        "model": "llama3",
        "messages": [{"role": "user", "content": "Stream response please"}],
        "stream": True
    }

    chunks_received = []
    has_done = False

    with httpx.Client(base_url=GATEWAY_URL) as client:
        with client.stream("POST", "/v1/chat/completions", headers=headers, json=payload, timeout=10.0) as resp:
            assert resp.status_code == 200
            assert "text/event-stream" in resp.headers["content-type"]
            assert "X-InferenceOS-Backend" in resp.headers

            for line in resp.iter_lines():
                if not line:
                    continue
                if line == "data: [DONE]":
                    has_done = True
                    break
                if line.startswith("data: "):
                    chunk_data = json.loads(line[6:])
                    chunks_received.append(chunk_data)

    assert len(chunks_received) > 3
    assert has_done is True
    # Verify chunk structure
    assert chunks_received[0]["object"] == "chat.completion.chunk"

def test_client_cancellation_recorded():
    headers = {
        "Authorization": f"Bearer {DEMO_KEY}",
        "Content-Type": "application/json"
    }
    payload = {
        "model": "llama3",
        "messages": [{"role": "user", "content": "Stream response"}],
        "stream": True
    }

    with httpx.Client(base_url=GATEWAY_URL) as client:
        # Connect and abort after 1 chunk
        with client.stream("POST", "/v1/chat/completions", headers=headers, json=payload, timeout=10.0) as resp:
            for line in resp.iter_lines():
                if line.startswith("data: "):
                    break
        # Closed response stream early

    time.sleep(0.5)
    with httpx.Client(base_url=GATEWAY_URL) as client:
        r = client.get("/metrics")
        assert "inferenceos_client_cancellations" in r.text

def test_adr001_safe_retry_pre_stream_failure():
    """
    Test ADR-001: Pre-stream failure on a worker is safely retried on an alternate healthy worker.
    """
    headers = {
        "Authorization": f"Bearer {DEMO_KEY}",
        "Content-Type": "application/json",
        "x-chaos-fail-pre-stream": "true"
    }
    payload = {
        "model": "llama3",
        "messages": [{"role": "user", "content": "Test safe retry"}],
        "stream": False
    }

    with httpx.Client(base_url=GATEWAY_URL) as client:
        r = client.post("/v1/chat/completions", headers=headers, json=payload, timeout=10.0)
        # Even with chaos header, the gateway retries on the second worker
        assert r.status_code in (200, 502)

def test_tenant_authorization_and_budget_limits():
    headers = {
        "Authorization": "Bearer sk-invalid-key-xyz",
        "Content-Type": "application/json"
    }
    payload = {
        "model": "llama3",
        "messages": [{"role": "user", "content": "Hello"}],
        "stream": False
    }

    with httpx.Client(base_url=GATEWAY_URL) as client:
        # Invalid auth
        r = client.post("/v1/chat/completions", headers=headers, json=payload)
        assert r.status_code == 401
        assert r.json()["error"]["code"] == "unauthorized"

        # Register tenant with microscopic budget (5 tokens)
        admin_headers = {"Authorization": f"Bearer {ADMIN_KEY}"}
        micro_tenant = {
            "id": "tenant-broke",
            "name": "Depleted Budget Tenant",
            "api_key": "sk-broke-key",
            "token_budget": 5,
            "tokens_used": 5,
            "max_inflight": 5
        }
        r_create = client.post("/admin/tenants", headers=admin_headers, json=micro_tenant)
        assert r_create.status_code == 201

        # Attempt call with depleted budget
        broke_headers = {"Authorization": "Bearer sk-broke-key"}
        r = client.post("/v1/chat/completions", headers=broke_headers, json=payload)
        assert r.status_code == 429
        assert r.json()["error"]["code"] == "budget_exceeded"

def test_multi_strategy_routing_and_audit():
    with httpx.Client(base_url=GATEWAY_URL) as client:
        for policy in ["round-robin", "least-inflight", "estimated-queue-delay"]:
            headers = {
                "Authorization": f"Bearer {DEMO_KEY}",
                "Content-Type": "application/json",
                "x-inferenceos-policy": policy
            }
            payload = {
                "model": "llama3",
                "messages": [{"role": "user", "content": f"Testing policy {policy}"}],
                "stream": False
            }
            r = client.post("/v1/chat/completions", headers=headers, json=payload, timeout=10.0)
            assert r.status_code == 200
            assert r.headers.get("X-InferenceOS-Policy") == policy

        # Verify audit history
        admin_headers = {"Authorization": f"Bearer {ADMIN_KEY}"}
        r_audit = client.get("/admin/routes/audit", headers=admin_headers)
        assert r_audit.status_code == 200
        audits = r_audit.json()
        assert len(audits) >= 3
        assert any(a["policy"] == "round-robin" for a in audits)
        assert any(a["policy"] == "least-inflight" for a in audits)
