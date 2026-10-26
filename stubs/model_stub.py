"""
Deterministic OpenAI-compatible model server stub.
Zero external dependencies (uses standard library http.server).
Simulates realistic TTFT, chunk streaming, usage reporting, and chaos injection.
"""

import argparse
import json
import sys
import time
import uuid
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

class StubHandler(BaseHTTPRequestHandler):
    name: str = "generic-stub"
    ttft_ms: float = 30.0
    tok_ms: float = 15.0
    fail_rate: float = 0.0

    def log_message(self, format, *args):
        # Concise logging
        sys.stderr.write(f"[{self.name}] {self.address_string()} - {format % args}\n")

    def do_GET(self):
        if self.path in ("/healthz", "/readyz", "/"):
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            resp = {
                "status": "healthy",
                "worker": self.name,
                "ttft_ms": self.ttft_ms,
                "tok_ms": self.tok_ms,
            }
            self.wfile.write(json.dumps(resp).encode("utf-8"))
        elif self.path == "/v1/models":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            models = {
                "object": "list",
                "data": [
                    {"id": "llama3", "object": "model", "owned_by": "local"},
                    {"id": "qwen2.5", "object": "model", "owned_by": "local"},
                    {"id": "mistral", "object": "model", "owned_by": "local"}
                ]
            }
            self.wfile.write(json.dumps(models).encode("utf-8"))
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        if self.path != "/v1/chat/completions":
            self.send_response(404)
            self.end_headers()
            return

        content_length = int(self.headers.get("Content-Length", 0))
        post_data = self.rfile.read(content_length)
        try:
            req = json.loads(post_data.decode("utf-8")) if post_data else {}
        except Exception:
            req = {}

        # Chaos headers
        chaos_fail_pre = self.headers.get("x-chaos-fail-pre-stream")
        chaos_fail_mid = self.headers.get("x-chaos-fail-mid-stream")
        chaos_delay = self.headers.get("x-chaos-delay-ms")

        # 1. Check pre-stream failure chaos
        if chaos_fail_pre:
            self.send_response(500)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"error":{"message":"Injected pre-stream worker failure","type":"server_error"}}')
            return

        # Effective TTFT
        effective_ttft = float(chaos_delay) / 1000.0 if chaos_delay else (self.ttft_ms / 1000.0)
        time.sleep(effective_ttft)

        model = req.get("model", "llama3")
        is_stream = req.get("stream", False)
        req_id = f"chatcmpl-{uuid.uuid4().hex[:12]}"
        created = int(time.time())

        # Sample words to stream
        response_words = [
            "InferenceOS", "provides", "high-performance", "control-plane",
            "scheduling,", "fairness", "guarantees,", "and", "predictable",
            "latency", "for", "modern", "AI", "serving."
        ]

        if not is_stream:
            # Non-streaming JSON response
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("X-InferenceOS-Backend", self.name)
            self.end_headers()
            full_text = " ".join(response_words)
            prompt_toks = 15
            comp_toks = len(response_words)
            payload = {
                "id": req_id,
                "object": "chat.completion",
                "created": created,
                "model": model,
                "choices": [{
                    "index": 0,
                    "message": {"role": "assistant", "content": full_text},
                    "finish_reason": "stop"
                }],
                "usage": {
                    "prompt_tokens": prompt_toks,
                    "completion_tokens": comp_toks,
                    "total_tokens": prompt_toks + comp_toks
                }
            }
            self.wfile.write(json.dumps(payload).encode("utf-8"))
            return

        # Streaming SSE response
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.send_header("X-InferenceOS-Backend", self.name)
        self.end_headers()

        # Send initial role delta
        first_chunk = {
            "id": req_id,
            "object": "chat.completion.chunk",
            "created": created,
            "model": model,
            "choices": [{"index": 0, "delta": {"role": "assistant", "content": ""}, "finish_reason": None}]
        }
        self.wfile.write(f"data: {json.dumps(first_chunk)}\n\n".encode("utf-8"))
        self.wfile.flush()

        # Stream words
        inter_token_delay = self.tok_ms / 1000.0
        for i, word in enumerate(response_words):
            # Check mid-stream failure injection
            if chaos_fail_mid and i >= 3:
                # Abruptly drop TCP connection to test Safe Retry Invariant
                sys.stderr.write(f"[{self.name}] Injecting mid-stream socket crash at token {i}\n")
                self.close_connection = True
                try:
                    self.connection.close()
                except Exception:
                    pass
                return

            time.sleep(inter_token_delay)
            chunk = {
                "id": req_id,
                "object": "chat.completion.chunk",
                "created": created,
                "model": model,
                "choices": [{"index": 0, "delta": {"content": (" " if i > 0 else "") + word}, "finish_reason": None}]
            }
            try:
                self.wfile.write(f"data: {json.dumps(chunk)}\n\n".encode("utf-8"))
                self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError):
                sys.stderr.write(f"[{self.name}] Client aborted connection early.\n")
                return

        # Send final chunk with finish_reason
        final_chunk = {
            "id": req_id,
            "object": "chat.completion.chunk",
            "created": created,
            "model": model,
            "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
            "usage": {
                "prompt_tokens": 15,
                "completion_tokens": len(response_words),
                "total_tokens": 15 + len(response_words)
            }
        }
        self.wfile.write(f"data: {json.dumps(final_chunk)}\n\n".encode("utf-8"))
        self.wfile.write(b"data: [DONE]\n\n")
        self.wfile.flush()

def make_handler(name: str, ttft_ms: float, tok_ms: float):
    class CustomHandler(StubHandler):
        pass
    CustomHandler.name = name
    CustomHandler.ttft_ms = ttft_ms
    CustomHandler.tok_ms = tok_ms
    return CustomHandler

def run_server(port: int, name: str, ttft_ms: float, tok_ms: float):
    handler = make_handler(name, ttft_ms, tok_ms)
    server = ThreadingHTTPServer(("0.0.0.0", port), handler)
    server.daemon_threads = True
    print(f"[{name}] Listening on http://0.0.0.0:{port} (TTFT: {ttft_ms}ms, tok: {tok_ms}ms/t)")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Deterministic OpenAI-compatible model stub")
    parser.add_argument("--port", type=int, default=8001, help="Port to bind")
    parser.add_argument("--name", type=str, default="stub-fast", help="Worker name")
    parser.add_argument("--ttft-ms", type=float, default=25.0, help="Simulated TTFT in ms")
    parser.add_argument("--tok-ms", type=float, default=12.0, help="Simulated per-token delay in ms")
    args = parser.parse_args()

    run_server(args.port, args.name, args.ttft_ms, args.tok_ms)
