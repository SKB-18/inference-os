"""
Runs deterministic fast and slow worker stubs simultaneously in daemon threads.
Used for multi-worker local testing and benchmarks.
"""

import sys
import threading
import time
from model_stub import run_server

def main():
    workers = [
        {"port": 8001, "name": "worker-fast", "ttft_ms": 20.0, "tok_ms": 6.0},
        {"port": 8002, "name": "worker-slow", "ttft_ms": 75.0, "tok_ms": 18.0},
        {"port": 8003, "name": "worker-chaos", "ttft_ms": 30.0, "tok_ms": 10.0},
    ]

    threads = []
    for w in workers:
        t = threading.Thread(
            target=run_server,
            args=(w["port"], w["name"], w["ttft_ms"], w["tok_ms"]),
            daemon=True
        )
        t.start()
        threads.append(t)

    print("=== InferenceOS Model Stub Cluster Running ===")
    print("  Fast Worker:  http://127.0.0.1:8001/v1 (TTFT: 30ms, tok: 10ms)")
    print("  Slow Worker:  http://127.0.0.1:8002/v1 (TTFT: 250ms, tok: 45ms)")
    print("  Chaos Worker: http://127.0.0.1:8003/v1 (TTFT: 60ms, tok: 20ms)")
    print("Press Ctrl+C to stop.")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping stub cluster...")

if __name__ == "__main__":
    main()
