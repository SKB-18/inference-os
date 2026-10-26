import os
import subprocess
import sys
import time
import httpx
import pytest

GATEWAY_PORT = 8090
GATEWAY_URL = f"http://127.0.0.1:{GATEWAY_PORT}"
ADMIN_KEY = "admin-secret-key-123"

@pytest.fixture(scope="session", autouse=True)
def run_cluster_and_gateway():
    # 1. Start model stub cluster
    stub_script = os.path.join(os.path.dirname(__file__), "..", "stubs", "run_cluster.py")
    stub_proc = subprocess.Popen(
        [sys.executable, stub_script],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )

    # 2. Wait for stubs to bind
    time.sleep(1.5)

    # 3. Start InferenceOS gateway executable
    exe_path = os.path.join(os.path.dirname(__file__), "..", "target", "debug", "inference-os.exe")
    env = os.environ.copy()
    env["INFERENCEOS_PORT"] = str(GATEWAY_PORT)
    env["INFERENCEOS_ADMIN_KEY"] = ADMIN_KEY
    env["INFERENCEOS_ALLOW_LOCAL_BACKENDS"] = "true"

    gateway_proc = subprocess.Popen(
        [exe_path],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )

    # 4. Wait for gateway to become healthy
    ready = False
    for _ in range(30):
        try:
            r = httpx.get(f"{GATEWAY_URL}/healthz", timeout=1.0)
            if r.status_code == 200:
                ready = True
                break
        except Exception:
            pass
        time.sleep(0.5)

    if not ready:
        stub_proc.kill()
        gateway_proc.kill()
        raise RuntimeError("InferenceOS gateway failed to start within timeout.")

    yield

    # Teardown
    gateway_proc.terminate()
    stub_proc.terminate()
    try:
        gateway_proc.wait(timeout=2)
        stub_proc.wait(timeout=2)
    except Exception:
        gateway_proc.kill()
        stub_proc.kill()
