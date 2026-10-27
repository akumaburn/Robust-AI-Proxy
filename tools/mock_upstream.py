#!/usr/bin/env python3
"""Mock OpenAI-style upstream for local testing of Robust-AI-Proxy.

Endpoints (base path /v1):
  POST /v1/chat/completions  stream=true   -> SSE chunks; the FIRST streaming
                                             call stalls 12 s mid-stream (to
                                             exercise the proxy's idle timeout)
  POST /v1/chat/completions  stream=false  -> JSON completion
  POST /v1/bad                           -> 404 JSON (deterministic error)
  POST /v1/limit-body                    -> 429 with "reset at <ts>" body (first call only)
  POST /v1/limit-ra                      -> 429 with Retry-After header (first call only)
  POST /v1/limit-plain                   -> 429 with no reset info, always
  GET  /v1/models                        -> JSON list

Listens on 127.0.0.1:9911.
"""
import datetime
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

STALL_SECONDS = 12.0
_lock = threading.Lock()
_state = {"stalled": False, "limit_body": False, "limit_ra": False}


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def _read_body(self):
        n = int(self.headers.get("Content-Length") or 0)
        return json.loads(self.rfile.read(n) or b"{}") if n else {}

    def _send(self, status, content_type, payload):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_POST(self):
        req = self._read_body()
        if self.path == "/v1/limit-body":
            # first call: 429 with a reset timestamp in the body (+3s)
            with _lock:
                if not _state["limit_body"]:
                    _state["limit_body"] = True
                    reset = (datetime.datetime.now() + datetime.timedelta(seconds=3)).strftime(
                        "%Y-%m-%d %H:%M:%S")
                    self._send(429, "application/json", json.dumps({
                        "error": {"message": f"Usage limit reached for 5 hour. "
                                             f"Your limit will reset at {reset}",
                                  "type": "rate_limit_error"}}).encode())
                    return
        if self.path == "/v1/limit-ra":
            # first call: 429 with a Retry-After header
            with _lock:
                if not _state["limit_ra"]:
                    _state["limit_ra"] = True
                    self.send_response(429)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Retry-After", "3")
                    self.send_header("Content-Length", "2")
                    self.end_headers()
                    self.wfile.write(b"{}")
                    return
        if self.path == "/v1/limit-plain":
            # always: 429 with no reset info at all
            self._send(429, "application/json", json.dumps(
                {"error": {"message": "slow down (mock)", "type": "rate_limit_error"}}).encode())
            return
        if self.path == "/v1/bad":
            self._send(404, "application/json", json.dumps(
                {"error": {"message": "not found (mock)", "type": "invalid_request_error"}}).encode())
            return
        if self.path == "/v1/chat/completions" and req.get("stream"):
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Connection", "close")
            self.close_connection = True
            self.end_headers()
            for i in range(5):
                chunk = {"id": "cmpl-1", "object": "chat.completion.chunk",
                         "choices": [{"index": 0, "delta": {"content": f"tok{i} "}}]}
                self.wfile.write(f"data: {json.dumps(chunk)}\n\n".encode())
                self.wfile.flush()
                if i == 1:
                    with _lock:
                        should_stall = not _state["stalled"]
                        if should_stall:
                            _state["stalled"] = True
                    if should_stall:
                        time.sleep(STALL_SECONDS)
                time.sleep(0.4)
            self.wfile.write(b"data: [DONE]\n\n")
            self.wfile.flush()
            return
        payload = json.dumps({
            "id": "cmpl-1", "object": "chat.completion",
            "choices": [{"index": 0,
                         "message": {"role": "assistant", "content": "hello from mock"},
                         "finish_reason": "stop"}]}).encode()
        self._send(200, "application/json", payload)

    def do_GET(self):
        if self.path == "/v1/models":
            self._send(200, "application/json", json.dumps(
                {"object": "list", "data": [{"id": "mock-1"}]}).encode())
            return
        self._send(404, "application/json",
                   b'{"error":{"message":"not found (mock)","type":"invalid_request_error"}}')


if __name__ == "__main__":
    server = ThreadingHTTPServer(("127.0.0.1", 9911), Handler)
    print("mock upstream on http://127.0.0.1:9911/v1", flush=True)
    server.serve_forever()
