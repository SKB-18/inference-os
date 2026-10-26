"""
Benchmark Suite for InferenceOS.
Compares Round-Robin vs Queue-Aware (Estimated Queue Delay) Scheduling.
Measures TTFT (p50, p90, p95, p99), throughput (tokens/sec, reqs/min), and failure recovery.
"""

import concurrent.futures
import json
import os
import subprocess
import sys
import time
import httpx

GATEWAY_URL = "http://127.0.0.1:8090"
DEMO_KEY = "sk-inferenceos-demo-key"

def run_single_request(client, policy, prompt, req_idx):
    headers = {
        "Authorization": f"Bearer {DEMO_KEY}",
        "Content-Type": "application/json",
        "x-inferenceos-policy": policy
    }
    payload = {
        "model": "llama3",
        "messages": [{"role": "user", "content": prompt}],
        "stream": True
    }

    start_time = time.perf_counter()
    first_token_time = None
    tokens_count = 0
    backend_used = None

    try:
        with client.stream("POST", f"{GATEWAY_URL}/v1/chat/completions", headers=headers, json=payload, timeout=12.0) as resp:
            if resp.status_code != 200:
                return None
            backend_used = resp.headers.get("X-InferenceOS-Backend", "unknown")

            for line in resp.iter_lines():
                if not line:
                    continue
                if line.startswith("data: "):
                    if first_token_time is None:
                        first_token_time = time.perf_counter()
                    data_str = line[6:]
                    if data_str == "[DONE]":
                        break
                    tokens_count += 1

        total_time = time.perf_counter() - start_time
        ttft_ms = ((first_token_time - start_time) * 1000.0) if first_token_time else (total_time * 1000.0)
        return {
            "req_idx": req_idx,
            "policy": policy,
            "backend": backend_used,
            "ttft_ms": ttft_ms,
            "total_ms": total_time * 1000.0,
            "tokens": tokens_count
        }
    except Exception as e:
        return None

def run_workload(policy: str, num_requests: int = 40, concurrency: int = 8):
    prompts = [
        "Short question: What is 2 + 2?",
        "Medium prompt: Explain the difference between process and thread scheduling.",
        "Long prompt: Analyze the memory layout of modern NUMA architectures and LLM KV cache management.",
        "Summarize the Paxos consensus algorithm in distributed systems.",
    ]

    print(f"--> Running workload for policy: {policy} ({num_requests} requests, concurrency={concurrency})...")
    results = []
    start = time.perf_counter()

    with httpx.Client(timeout=15.0) as client:
        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = [
                executor.submit(run_single_request, client, policy, prompts[i % len(prompts)], i)
                for i in range(num_requests)
            ]
            for f in concurrent.futures.as_completed(futures):
                res = f.result()
                if res:
                    results.append(res)

    duration = time.perf_counter() - start
    ttft_values = sorted([r["ttft_ms"] for r in results])
    total_tokens = sum(r["tokens"] for r in results)

    def p(vals, pct):
        if not vals:
            return 0.0
        idx = int(len(vals) * pct / 100.0)
        return vals[min(idx, len(vals) - 1)]

    stats = {
        "policy": policy,
        "completed": len(results),
        "target_requests": num_requests,
        "duration_s": round(duration, 2),
        "tokens_per_sec": round(total_tokens / max(duration, 0.001), 1),
        "requests_per_min": round((len(results) / max(duration, 0.001)) * 60, 1),
        "ttft_p50_ms": round(p(ttft_values, 50), 2),
        "ttft_p90_ms": round(p(ttft_values, 90), 2),
        "ttft_p95_ms": round(p(ttft_values, 95), 2),
        "ttft_p99_ms": round(p(ttft_values, 99), 2),
        "backend_distribution": {}
    }

    for r in results:
        b = r["backend"]
        stats["backend_distribution"][b] = stats["backend_distribution"].get(b, 0) + 1

    return stats

def main():
    print("==============================================================")
    print("      InferenceOS Empirical Benchmark: Scheduler Comparison   ")
    print("==============================================================")

    stub_proc = None
    gateway_proc = None

    # Check if gateway is reachable, else start local cluster
    try:
        r = httpx.get(f"{GATEWAY_URL}/healthz", timeout=1.0)
        is_running = (r.status_code == 200)
    except Exception:
        is_running = False

    if not is_running:
        print("[*] Launching stub cluster and InferenceOS gateway...")
        stub_script = os.path.join(os.path.dirname(__file__), "..", "stubs", "run_cluster.py")
        stub_proc = subprocess.Popen([sys.executable, stub_script], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(1.0)

        exe_path = os.path.join(os.path.dirname(__file__), "..", "target", "debug", "inference-os.exe")
        env = os.environ.copy()
        env["INFERENCEOS_PORT"] = "8090"
        env["INFERENCEOS_ALLOW_LOCAL_BACKENDS"] = "true"
        gateway_proc = subprocess.Popen([exe_path], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        for _ in range(25):
            try:
                r = httpx.get(f"{GATEWAY_URL}/healthz", timeout=1.0)
                if r.status_code == 200:
                    break
            except Exception:
                pass
            time.sleep(0.4)

    try:
        # 1. Warmup
        print("[*] Warming up gateway connections...")
        with httpx.Client(timeout=5.0) as client:
            for _ in range(4):
                run_single_request(client, "round-robin", "Warmup ping", 0)

        time.sleep(0.5)

        # 2. Run Round-Robin baseline
        rr_stats = run_workload("round-robin", num_requests=30, concurrency=6)

        time.sleep(0.5)

        # 3. Run Queue-Aware (Estimated Queue Delay)
        eqd_stats = run_workload("estimated-queue-delay", num_requests=30, concurrency=6)

        # 4. Compute Improvement
        ttft_diff_pct = 0.0
        if rr_stats["ttft_p95_ms"] > 0:
            ttft_diff_pct = ((rr_stats["ttft_p95_ms"] - eqd_stats["ttft_p95_ms"]) / rr_stats["ttft_p95_ms"]) * 100.0

        print("\n================== BENCHMARK COMPARISON REPORT ==================")
        print(f"{'Metric':<28} | {'Round-Robin (Baseline)':<22} | {'Queue-Aware (EQD)':<20}")
        print("-" * 76)
        print(f"{'Completed Requests':<28} | {rr_stats['completed']:<22} | {eqd_stats['completed']:<20}")
        print(f"{'TTFT p50 (ms)':<28} | {rr_stats['ttft_p50_ms']:<22} | {eqd_stats['ttft_p50_ms']:<20}")
        print(f"{'TTFT p90 (ms)':<28} | {rr_stats['ttft_p90_ms']:<22} | {eqd_stats['ttft_p90_ms']:<20}")
        print(f"{'TTFT p95 (ms)':<28} | {rr_stats['ttft_p95_ms']:<22} | {eqd_stats['ttft_p95_ms']:<20}")
        print(f"{'TTFT p99 (ms)':<28} | {rr_stats['ttft_p99_ms']:<22} | {eqd_stats['ttft_p99_ms']:<20}")
        print(f"{'Throughput (tokens/sec)':<28} | {rr_stats['tokens_per_sec']:<22} | {eqd_stats['tokens_per_sec']:<20}")
        print(f"{'Throughput (reqs/min)':<28} | {rr_stats['requests_per_min']:<22} | {eqd_stats['requests_per_min']:<20}")
        print(f"{'Backend Distribution':<28} | {str(rr_stats['backend_distribution']):<22} | {str(eqd_stats['backend_distribution']):<20}")
        print("-" * 76)
        print(f"p95 TTFT Latency Improvement: {ttft_diff_pct:+.1f}%")
        print("=================================================================\n")

        # Save to JSON
        os.makedirs(os.path.join(os.path.dirname(__file__), "results"), exist_ok=True)
        summary_path = os.path.join(os.path.dirname(__file__), "results", "benchmark_summary.json")
        with open(summary_path, "w") as f:
            json.dump({
                "round_robin": rr_stats,
                "queue_aware": eqd_stats,
                "p95_improvement_pct": round(ttft_diff_pct, 2)
            }, f, indent=2)
        print(f"Results archived to {summary_path}")

    finally:
        if gateway_proc:
            gateway_proc.terminate()
        if stub_proc:
            stub_proc.terminate()

if __name__ == "__main__":
    main()
