# Robust-AI-Proxy

A small OpenAI-compatible API reverse proxy in standalone Java (JDK 17+,
zero external dependencies). It sits between your client and an LLM
provider, forwards requests, monitors streaming activity, and transparently
retries the upstream request when the provider stops making progress.
The client connection stays open the whole time; the client never sees the
retry.

Listens on **port 8000** of the current machine by default.

## Why an idle timeout, not a total timeout

- **Idle timeout** (default: 5 minutes): the proxy aborts an upstream
  attempt when no response bytes arrive for that long, then retries.
  A long, healthy generation keeps ticking off SSE events, so it is never
  cut. A dead connection is cut and replaced.
- **Total timeout** (default: off): a hard cap on the whole request.
  Capping long generations is usually undesirable, so it is disabled
  unless you opt in.

A stalled attempt is discarded entirely and the request is re-sent to the
upstream from the beginning. Because OpenAI-style streams cannot be
resumed mid-way, the client always receives **one clean, complete
stream** — never a partial one and never duplicated tokens.

## Delivery modes

`RAI_PROXY_STREAM_MODE` selects how the response reaches the client:

- **`buffer`** (default): the proxy accumulates the upstream response and
  forwards it to the client only after the upstream stream completes
  cleanly (for SSE, after `data: [DONE]`). Retries are fully transparent
  for the whole request lifetime. Trade-off: the client's first byte
  arrives when the generation finishes, so a cancelled client still burns
  upstream tokens until completion.
- **`passthrough`**: bytes are forwarded as they arrive (real
  time-to-first-token). Retries apply until the client has received the
  response headers; once streaming to the client has started, a stall
  closes the stream instead of retrying, because restarting upstream would
  duplicate tokens the client already saw. 5xx responses are captured in
  memory and retried before anything is shown to the client.

## Full HTTP transparency

The proxy is method-, path- and content-type-agnostic. Anything an
OpenAI-compatible backend serves, it forwards:

- **Methods**: `GET`, `POST`, `PUT`, `DELETE`, `PATCH`, `HEAD`, `OPTIONS`
  all pass through (verified end to end).
- **Paths and query strings** are appended to the upstream base verbatim,
  so the entire API surface of the backend is reachable:
  `/chat/completions`, `/completions`, `/embeddings`, `/audio/*`,
  `/images/*`, `/files`, `/models`, fine-tuning, and anything else the
  backend exposes.
- **Bodies** are forwarded byte-exact in both directions: JSON, SSE,
  `multipart/form-data` (file uploads, audio), and binary responses
  (images, audio) all work unmodified.

Responses are framed as strict HTTP/1.1 so any client can parse them:

- hop-by-hop and framing headers are stripped and re-set by the proxy
  (`Content-Length` / chunked as appropriate);
- HTTP/2 pseudo-headers (`:status` and friends) from an H2 upstream are
  dropped — strict parsers (undici/Node fetch, curl, browsers) reject them;
- header values are sanitized of CR/LF and control characters so no
  forwarded header can split into a bogus line on the wire.

The same proxy fronts any number of backends — OpenAI, Z.ai, a local vLLM,
SGLang, or llama.cpp `server` — by setting `RAI_PROXY_UPSTREAM` per
process or `X-Proxy-Upstream` per request.

## Retry rules

Retried:

- idle timeout (no upstream bytes for `RAI_PROXY_IDLE_TIMEOUT_SECONDS`)
- total timeout, when enabled
- transport errors (connection refused, reset, DNS, TLS)
- upstream 5xx responses
- SSE stream that ended without `data: [DONE]` (buffer mode only)

Not retried:

- upstream 4xx responses — forwarded to the client verbatim
- anything after the client has received response headers (passthrough)

Budget: `RAI_PROXY_MAX_RETRIES` (default unlimited, `0` disables
retries). A fixed `RAI_PROXY_RETRY_BACKOFF_MS` (default 500) delay
separates attempts. When the budget is exhausted, the client gets an
OpenAI-style 502:

```json
{"error":{"message":"upstream failed after 3 attempt(s): ...",
          "type":"proxy_upstream_error","code":"upstream_failed"}}
```

Responses carry an `X-Proxy-Attempts` header showing how many upstream
attempts were made.

## Quick start

Open the folder in IntelliJ IDEA (the `.iml` marks `src` as the source
root), pick a JDK 17+ project SDK, and run
`com.robust.aiproxy.Main`. Set the upstream (and key) for your run
configuration, e.g. via environment variables:

```
RAI_PROXY_UPSTREAM=https://api.openai.com/v1
RAI_PROXY_API_KEY=sk-...
```

Or from a shell:

```sh
./run.sh
```

Point any OpenAI-compatible client at `http://localhost:8000`:

```sh
curl http://localhost:8000/chat/completions \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer whatever' \
  -d '{"model":"gpt-4o","stream":true,
       "messages":[{"role":"user","content":"Hello"}]}'
```

**Path convention.** The proxy forwards `RAI_PROXY_UPSTREAM + client_path`
verbatim. The upstream base already carries the version prefix (e.g.
`https://api.openai.com/v1`, `https://api.z.ai/api/paas/v4`), so the client
path must **not** repeat it: call `/chat/completions`, not
`/v1/chat/completions`. If your upstream base has no version prefix (e.g. a
bare vLLM host), include it in the client path instead — the rule is simply
"whatever the upstream server expects after its base URL".

`/health` returns proxy status without touching the upstream.

## Configuration

System properties (`-Dname=...`) override environment variables
(`RAI_PROXY_*`), which override defaults.

| Property / env var | Default | Meaning |
|---|---|---|
| `port` / `RAI_PROXY_PORT` | `8000` | listen port |
| `bind` / `RAI_PROXY_BIND` | `0.0.0.0` | listen address |
| `upstream` / `RAI_PROXY_UPSTREAM` | `https://api.openai.com/v1` | upstream base URL |
| `apiKey` / `RAI_PROXY_API_KEY` | *(empty)* | sent as `Authorization: Bearer ...`, replacing the client's header. Empty = forward the client's header |
| `idleTimeoutSeconds` / `RAI_PROXY_IDLE_TIMEOUT_SECONDS` | `300` | abort + retry when no upstream bytes for this long |
| `totalTimeoutSeconds` / `RAI_PROXY_TOTAL_TIMEOUT_SECONDS` | `0` (off) | hard cap on the whole request |
| `maxRetries` / `RAI_PROXY_MAX_RETRIES` | `-1` (unlimited) | retries after the first attempt; `0` = none |
| `retryBackoffMs` / `RAI_PROXY_RETRY_BACKOFF_MS` | `500` | delay between attempts |
| `streamMode` / `RAI_PROXY_STREAM_MODE` | `buffer` | `buffer` or `passthrough` |
| `requireDone` / `RAI_PROXY_REQUIRE_DONE` | `true` | treat a stream ending without `data: [DONE]` as incomplete (buffer mode) |

Per-request overrides:

- `X-Proxy-Upstream: https://other-provider/v1` — route this request to a
  different upstream base.
- `X-Proxy-Api-Key: sk-...` — use this key for this request instead of the
  configured one.

Client requests are forwarded with hop-by-hop headers stripped and
`Accept-Encoding: identity` set, so SSE bytes reach the proxy unencoded.
The client's `Authorization` is replaced by `RAI_PROXY_API_KEY` when that
is set; otherwise it is passed through.

## Local test without a provider

`tools/mock_upstream.py` is a mock OpenAI-style upstream on
`127.0.0.1:9911`. Its first streaming call stalls 12 s mid-stream, which
exercises the idle-timeout retry. With a 3 s idle limit:

```sh
python3 tools/mock_upstream.py &
RAI_PROXY_UPSTREAM=http://127.0.0.1:9911 \
RAI_PROXY_IDLE_TIMEOUT_SECONDS=3 \
RAI_PROXY_PORT=8000 ./run.sh &

curl -N http://localhost:8000/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"model":"mock","stream":true,"messages":[{"role":"user","content":"hi"}]}'
```

Expect one complete stream ending in `data: [DONE]` and an
`X-Proxy-Attempts: 2` header; the proxy log shows
`attempt 1 failed (idle timeout: no upstream bytes for 3s); retrying`.

## Assumptions and limits

- Request and response bodies are held in memory. LLM request bodies are
  small; a single very long generation is the only unbounded buffer.
- In buffer mode a client that cancels is invisible to the proxy until the
  generation completes.
- `stream` detection scans the JSON body for a `"stream": true` member; a
  provider that ignores the flag and streams anyway is handled by the
  `[DONE]` check in buffer mode.
- The proxy does not rewrite models, add caching, or multiplex keys; it
  is deliberately a thin, robust forwarder.

## Project layout

```
src/com/robust/aiproxy/
  Main.java            entry point, startup, shutdown hook
  Config.java          env/-D configuration with defaults
  ProxyServer.java     com.sun.net.httpserver wiring, shared HttpClient
  ProxyHandler.java    request pipeline, retry policy, both modes
  UpstreamClient.java  one upstream attempt + idle/total-timeout watchdog
  SseUtil.java         stream-flag detection, [DONE] and SSE checks
  Log.java             single-line console logger
tools/mock_upstream.py local test upstream with a mid-stream stall
```
