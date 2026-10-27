package com.robust.aiproxy;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Handles one client request end to end:
 *
 * 1. Resolves the upstream target (base URL + client path + query).
 * 2. Builds the forwarded request (headers sanitized, API key injected).
 * 3. Runs the attempt loop with the retry policy.
 * 4. Writes the response back to the client.
 *
 * Two delivery modes exist:
 *
 * buffer (default) - the full upstream response is accumulated in memory
 *   and forwarded to the client only after the upstream stream completes
 *   cleanly. Mid-stream stalls are invisible to the client; a stalled
 *   attempt is discarded and retried, so the client always receives one
 *   clean, complete stream. Trade-off: first byte arrives only when the
 *   generation finishes.
 *
 * passthrough - bytes are forwarded to the client as they arrive (true
 *   time-to-first-token). Retries apply only before anything is written
 *   to the client; once the client has received headers, a stall closes
 *   the stream instead of retrying, because restarting the upstream would
 *   duplicate tokens the client already saw.
 */
public final class ProxyHandler implements HttpHandler {

    private static final Set<String> STRIP_REQUEST = Set.of(
            "host", "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
            "te", "trailer", "transfer-encoding", "upgrade", "content-length", "expect");
    private static final Set<String> STRIP_RESPONSE = Set.of(
            "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
            "te", "trailer", "transfer-encoding", "upgrade", "content-length");

    private final Config cfg;
    private final UpstreamClient upstream;

    public ProxyHandler(Config cfg, HttpClient http) {
        this.cfg = cfg;
        this.upstream = new UpstreamClient(http);
    }

    @Override
    public void handle(HttpExchange ex) {
        try {
            String path = ex.getRequestURI().getPath();
            if (path.equals("/health")) {
                replyJson(ex, 200, healthBody());
                return;
            }
            if (path.equals("/")) {
                replyJson(ex, 200, infoBody());
                return;
            }

            String method = ex.getRequestMethod();
            byte[] body = ex.getRequestBody().readAllBytes();
            boolean streaming = SseUtil.isStreamingRequest(body);
            URI target = resolveTarget(ex);
            List<Map.Entry<String, String>> headers = buildUpstreamHeaders(ex);

            Log.info("%s %s -> %s (stream=%s, %d bytes)",
                    method, ex.getRequestURI(), target, streaming, body.length);

            if (cfg.streamMode.equals("buffer")) {
                handleBuffered(ex, method, target, headers, body, streaming);
            } else {
                handlePassthrough(ex, method, target, headers, body, streaming);
            }
        } catch (Exception e) {
            Log.error("unhandled error for %s %s: %s", ex.getRequestMethod(), ex.getRequestURI(), e.toString());
            try {
                replyJson(ex, 500,
                        "{\"error\":{\"message\":\"proxy internal error: " + jsonEscape(e.toString()) + "\",\"type\":\"proxy_error\"}}");
            } catch (IOException ignored) {
            }
        } finally {
            ex.close();
        }
    }

    // ------------------------------------------------------------------
    // buffer mode
    // ------------------------------------------------------------------

    private void handleBuffered(HttpExchange ex, String method, URI target,
                                 List<Map.Entry<String, String>> headers, byte[] body,
                                 boolean streaming) throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        int attempts = 0;
        while (true) {
            attempts++;
            buf.reset();
            UpstreamClient.AttemptOutcome out = upstream.attempt(cfg, method, target, headers, body,
                    new UpstreamClient.ItemSink() {
                        @Override
                        public void onMeta(int s, Map<String, List<String>> h) {
                            // headers are reported in the outcome; nothing to do here
                        }

                        @Override
                        public void onChunk(byte[] b) {
                            buf.write(b, 0, b.length);
                        }
                    });

            switch (out.kind()) {
                case COMPLETED: {
                    int st = out.status();
                    byte[] bytes = buf.toByteArray();
                    if (st >= 400 && st < 500) {
                        if (st == 429 && cfg.rateLimitWait && canRetry(attempts)) {
                            long waitMs = rateLimitWaitMs(out.headers(), bytes);
                            if (waitMs > 0) {
                                long capped = Math.min(waitMs, cfg.maxRateLimitWaitMs);
                                Log.warn("attempt %d: 429 rate limit; holding client open, sleeping %ds until provider reset",
                                        attempts, capped / 1000);
                                sleepMs(capped);
                                continue;
                            }
                        }
                        // deterministic client error (or 429 without reset info): forward verbatim
                        sendBody(ex, st, out.headers(), bytes, attempts);
                        return;
                    }
                    if (st >= 500) {
                        if (canRetry(attempts)) {
                            Log.warn("attempt %d: upstream %d; retrying", attempts, st);
                            backoff();
                            continue;
                        }
                        sendBody(ex, st, out.headers(), bytes, attempts);
                        return;
                    }
                    if (streaming && cfg.requireDone && !SseUtil.endsWithDone(bytes)) {
                        if (SseUtil.looksLikeSse(bytes)) {
                            // an SSE stream that ended without [DONE] is incomplete
                            if (canRetry(attempts)) {
                                Log.warn("attempt %d: stream ended without [DONE]; retrying", attempts);
                                backoff();
                                continue;
                            }
                            Log.warn("attempt %d: no [DONE] after retries; forwarding partial stream", attempts);
                            sendBody(ex, st, out.headers(), bytes, attempts);
                            return;
                        }
                        // upstream answered with a non-SSE body; forward as-is
                    }
                    sendBody(ex, st, out.headers(), bytes, attempts);
                    return;
                }
                case IDLE_TIMEOUT:
                case TOTAL_TIMEOUT:
                case TRANSPORT_ERROR: {
                    if (canRetry(attempts)) {
                        Log.warn("attempt %d failed (%s); retrying", attempts, out.failReason());
                        backoff();
                        continue;
                    }
                    Log.error("giving up after %d attempt(s): %s", attempts, out.failReason());
                    fail502(ex, attempts, out.failReason());
                    return;
                }
                case CLIENT_GONE:
                    Log.warn("client disconnected; aborting buffered request");
                    return;
            }
        }
    }

    // ------------------------------------------------------------------
    // passthrough mode
    // ------------------------------------------------------------------

    private void handlePassthrough(HttpExchange ex, String method, URI target,
                                    List<Map.Entry<String, String>> headers, byte[] body,
                                    boolean streaming) throws IOException {
        int attempts = 0;
        while (true) {
            attempts++;
            PassthroughSink sink = new PassthroughSink(ex, attempts);
            UpstreamClient.AttemptOutcome out = upstream.attempt(cfg, method, target, headers, body, sink);
            switch (out.kind()) {
                case COMPLETED: {
                    if (sink.committed) {
                        return;
                    }
                    // captured 5xx / 429 body was not shown to the client
                    if (out.status() == 429 && cfg.rateLimitWait && canRetry(attempts)) {
                        long waitMs = rateLimitWaitMs(out.headers(), sink.capturedBody());
                        if (waitMs > 0) {
                            long capped = Math.min(waitMs, cfg.maxRateLimitWaitMs);
                            Log.warn("attempt %d: 429 rate limit; holding client open, sleeping %ds until provider reset",
                                    attempts, capped / 1000);
                            sleepMs(capped);
                            continue;
                        }
                    }
                    if (canRetry(attempts)) {
                        Log.warn("attempt %d: upstream %d; retrying", attempts, out.status());
                        backoff();
                        continue;
                    }
                    sink.flushDeferred();
                    return;
                }
                case IDLE_TIMEOUT:
                case TOTAL_TIMEOUT:
                case TRANSPORT_ERROR: {
                    if (sink.committed) {
                        // client already has a partial stream; restarting upstream
                        // would duplicate tokens, so close the stream cleanly
                        Log.warn("attempt %d: upstream stalled after the stream started; closing client stream (%s)",
                                attempts, out.failReason());
                        return;
                    }
                    if (canRetry(attempts)) {
                        Log.warn("attempt %d failed (%s); retrying", attempts, out.failReason());
                        backoff();
                        continue;
                    }
                    Log.error("giving up after %d attempt(s): %s", attempts, out.failReason());
                    fail502(ex, attempts, out.failReason());
                    return;
                }
                case CLIENT_GONE:
                    Log.warn("client disconnected; aborting passthrough request");
                    return;
            }
        }
    }

    /**
     * Passthrough sink. Commits client headers as soon as the upstream
     * headers arrive (unless the status is 5xx, in which case the body is
     * captured in memory so it can be retried, or forwarded once the
     * retry budget is exhausted).
     */
    private final class PassthroughSink implements UpstreamClient.ItemSink {
        private final HttpExchange ex;
        private final int attemptNumber;
        private boolean committed;
        private boolean clientGone;
        private int status;
        private Map<String, List<String>> responseHeaders = Map.of();
        private final ByteArrayOutputStream captured = new ByteArrayOutputStream();
        private OutputStream clientOut;

        PassthroughSink(HttpExchange ex, int attemptNumber) {
            this.ex = ex;
            this.attemptNumber = attemptNumber;
        }

        @Override
        public void onMeta(int s, Map<String, List<String>> h) {
            this.status = s;
            this.responseHeaders = h;
            if (s >= 500 || s == 429) {
                return; // capture the body; commit only if retries are exhausted
            }
            try {
                Headers resp = ex.getResponseHeaders();
                forwardResponseHeaders(resp, h);
                resp.set("X-Proxy-Attempts", String.valueOf(attemptNumber));
                ex.sendResponseHeaders(s, 0);
                clientOut = ex.getResponseBody();
                committed = true;
                Log.info("reply: status=%d (passthrough, streaming to client)", s);
            } catch (IOException ioe) {
                clientGone = true;
            }
        }

        @Override
        public void onChunk(byte[] b) {
            if (clientGone) {
                return;
            }
            if (committed) {
                try {
                    clientOut.write(b);
                    clientOut.flush();
                } catch (IOException ioe) {
                    clientGone = true;
                }
            } else if (captured.size() < (1 << 20)) {
                captured.write(b, 0, b.length);
            }
        }

        @Override
        public boolean clientGone() {
            return clientGone;
        }

        byte[] capturedBody() {
            return captured.toByteArray();
        }

        /** Forwards a captured 5xx / 429 response once retries are exhausted. */
        void flushDeferred() {
            byte[] b = captured.toByteArray();
            try {
                Headers resp = ex.getResponseHeaders();
                forwardResponseHeaders(resp, responseHeaders);
                resp.set("X-Proxy-Attempts", String.valueOf(attemptNumber));
                ex.sendResponseHeaders(status, b.length == 0 ? -1 : b.length);
                OutputStream os = ex.getResponseBody();
                os.write(b);
                os.close();
                Log.info("reply: status=%d attempts=%d bytes=%d (deferred 5xx)", status, attemptNumber, b.length);
            } catch (IOException ioe) {
                Log.warn("client gone while flushing deferred 5xx: %s", ioe.toString());
            }
        }
    }

    // ------------------------------------------------------------------
    // shared helpers
    // ------------------------------------------------------------------

    /** A retry is allowed while the number of retries used stays within the budget. */
    private boolean canRetry(int failedAttemptNumber) {
        int retriesUsed = failedAttemptNumber - 1;
        return cfg.maxRetries < 0 || retriesUsed < cfg.maxRetries;
    }

    private void backoff() {
        if (cfg.retryBackoffMs <= 0) {
            return;
        }
        sleepMs(cfg.retryBackoffMs);
    }

    private static void sleepMs(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    /** Timestamp as providers phrase it, e.g. "reset at 2026-10-28 02:52:28". */
    private static final Pattern RESET_AT =
            Pattern.compile("(\\d{4}-\\d{2}-\\d{2})[ T](\\d{2}:\\d{2}:\\d{2})");

    /**
     * Milliseconds until the upstream rate-limit window resets, for a 429
     * response. Resolution order:
     * 1. Retry-After header (delta-seconds or HTTP-date)
     * 2. a "YYYY-MM-DD HH:MM:SS" timestamp in the body, interpreted in the
     *    proxy's local timezone
     * Returns 0 when the reset time is unknown.
     */
    private static long rateLimitWaitMs(Map<String, List<String>> headers, byte[] body) {
        List<String> ra = headers.get("retry-after");
        if (ra != null && !ra.isEmpty()) {
            String v = ra.get(0).trim();
            if (!v.isEmpty()) {
                try {
                    long secs = Long.parseLong(v);
                    if (secs > 0) {
                        return secs * 1000;
                    }
                } catch (NumberFormatException notSeconds) {
                    try {
                        // HTTP-date form, e.g. "Mon, 26 Oct 2026 21:04:19 GMT"
                        OffsetDateTime odt =
                                OffsetDateTime.parse(v, DateTimeFormatter.RFC_1123_DATE_TIME);
                        long wait = odt.toInstant().toEpochMilli() - System.currentTimeMillis();
                        if (wait > 0) {
                            return wait;
                        }
                    } catch (Exception ignored) {
                    }
                }
            }
        }
        if (body != null && body.length > 0) {
            Matcher m = RESET_AT.matcher(new String(body, StandardCharsets.UTF_8));
            while (m.find()) {
                try {
                    LocalDateTime ldt = LocalDateTime.parse(m.group(0).replace(' ', 'T'));
                    long wait = ldt.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli()
                            - System.currentTimeMillis();
                    if (wait > 0) {
                        return wait;
                    }
                } catch (Exception ignored) {
                }
            }
        }
        return 0;
    }

    private URI resolveTarget(HttpExchange ex) {
        String base = cfg.upstream;
        String override = firstValue(ex.getRequestHeaders(), "X-Proxy-Upstream");
        if (override != null && !override.isBlank()) {
            base = override.trim();
        }
        while (base.endsWith("/")) {
            base = base.substring(0, base.length() - 1);
        }
        if (!base.startsWith("http://") && !base.startsWith("https://")) {
            throw new IllegalArgumentException("upstream base must be http(s): " + base);
        }
        String query = ex.getRequestURI().getRawQuery();
        String url = base + ex.getRequestURI().getPath() + (query == null ? "" : "?" + query);
        return URI.create(url);
    }

    private List<Map.Entry<String, String>> buildUpstreamHeaders(HttpExchange ex) {
        Headers in = ex.getRequestHeaders();
        String overrideKey = firstValue(in, "X-Proxy-Api-Key");
        String key = (overrideKey != null && !overrideKey.isBlank()) ? overrideKey.trim() : cfg.apiKey;
        boolean replaceAuth = key != null && !key.isEmpty();

        List<Map.Entry<String, String>> out = new ArrayList<>();
        for (String name : in.keySet()) {
            if (STRIP_REQUEST.contains(name.toLowerCase(Locale.ROOT))) {
                continue;
            }
            if (replaceAuth && name.equalsIgnoreCase("authorization")) {
                continue;
            }
            if (name.equalsIgnoreCase("accept-encoding")) {
                continue; // we request identity so [DONE] scanning stays byte-exact
            }
            for (String v : in.get(name)) {
                out.add(Map.entry(name, v));
            }
        }
        out.add(Map.entry("Accept-Encoding", "identity"));
        if (replaceAuth) {
            out.add(Map.entry("Authorization", "Bearer " + key));
        }
        out.add(Map.entry("X-Forwarded-For", ex.getRemoteAddress().getHostString()));
        return out;
    }

    private void forwardResponseHeaders(Headers out, Map<String, List<String>> upstreamHeaders) {
        for (Map.Entry<String, List<String>> e : upstreamHeaders.entrySet()) {
            String name = e.getKey();
            if (STRIP_RESPONSE.contains(name.toLowerCase(Locale.ROOT))) {
                continue;
            }
            if (!isValidHeaderName(name)) {
                // drops HTTP/2 pseudo-headers (":status" and friends) and any
                // other name that is not a valid HTTP/1.1 field-name; strict
                // clients (undici/llhttp) reject them with "Invalid header token"
                continue;
            }
            for (String v : e.getValue()) {
                String clean = sanitizeHeaderValue(v);
                if (!clean.isEmpty()) {
                    out.add(name, clean);
                }
            }
        }
    }

    /** RFC 7230 field-name: non-empty token, not starting with ":". */
    private static boolean isValidHeaderName(String name) {
        if (name == null || name.isEmpty() || name.charAt(0) == ':') {
            return false;
        }
        for (int i = 0; i < name.length(); i++) {
            if (!isTokenChar(name.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    private static boolean isTokenChar(char c) {
        if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')) {
            return true;
        }
        switch (c) {
            case '!': case '"': case '#': case '$': case '%': case '&': case '\'': case '*':
            case '+': case '-': case '.': case '/': case ':': case '<': case '=': case '>':
            case '^': case '_': case '`': case '|': case '~':
                return true;
            default:
                return false;
        }
    }

    /** Strips CR/LF and other control characters so a value can never
     *  split into a bogus header line on the wire. */
    private static String sanitizeHeaderValue(String v) {
        if (v == null) {
            return "";
        }
        StringBuilder sb = null;
        for (int i = 0; i < v.length(); i++) {
            char c = v.charAt(i);
            boolean keep = c == '\t' || (c >= 0x20 && c != 0x7f);
            if (!keep) {
                if (sb == null) {
                    sb = new StringBuilder(v.length());
                }
            } else if (sb != null) {
                sb.append(c);
            }
        }
        String out = sb == null ? v : sb.toString();
        return out.strip();
    }

    /** Writes a fully-buffered response to the client. */
    private void sendBody(HttpExchange ex, int status, Map<String, List<String>> headers,
                          byte[] body, int attempts) throws IOException {
        Headers resp = ex.getResponseHeaders();
        forwardResponseHeaders(resp, headers);
        if (resp.getFirst("Content-Type") == null) {
            resp.set("Content-Type",
                    SseUtil.looksLikeSse(body) ? "text/event-stream" : "application/json");
        }
        resp.set("X-Proxy-Attempts", String.valueOf(attempts));
        ex.sendResponseHeaders(status, body.length == 0 ? -1 : body.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(body);
        }
        Log.info("reply: status=%d attempts=%d bytes=%d content-type=%s",
                status, attempts, body.length, resp.getFirst("Content-Type"));
    }

    /** Terminal failure response, shaped like an OpenAI API error. */
    private void fail502(HttpExchange ex, int attempts, String reason) {
        try {
            String msg = "upstream failed after " + attempts + " attempt(s): " + reason;
            byte[] json = ("{\"error\":{\"message\":\"" + jsonEscape(msg)
                    + "\",\"type\":\"proxy_upstream_error\",\"code\":\"upstream_failed\"}}")
                    .getBytes(StandardCharsets.UTF_8);
            Headers resp = ex.getResponseHeaders();
            resp.set("Content-Type", "application/json");
            resp.set("X-Proxy-Attempts", String.valueOf(attempts));
            ex.sendResponseHeaders(502, json.length);
            try (OutputStream os = ex.getResponseBody()) {
                os.write(json);
            }
        } catch (IOException ioe) {
            Log.warn("client gone while sending 502: %s", ioe.toString());
        }
    }

    private void replyJson(HttpExchange ex, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        Headers resp = ex.getResponseHeaders();
        resp.set("Content-Type", "application/json");
        ex.sendResponseHeaders(status, bytes.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(bytes);
        }
    }

    private String healthBody() {
        return "{\"status\":\"ok\",\"proxy\":\"robust-ai-proxy\",\"version\":\"" + Main.VERSION + "\","
                + "\"upstream\":\"" + jsonEscape(cfg.upstream) + "\","
                + "\"stream_mode\":\"" + cfg.streamMode + "\","
                + "\"idle_timeout_seconds\":" + (cfg.idleTimeoutMs / 1000) + "}";
    }

    private String infoBody() {
        return "{\"proxy\":\"robust-ai-proxy\",\"version\":\"" + Main.VERSION + "\","
                + "\"description\":\"OpenAI-compatible reverse proxy with idle-timeout retries\","
                + "\"config\":" + jsonEscape(Config.describe(cfg)) + ","
                + "\"endpoints\":{\"health\":\"/health\",\"usage\":\"forward any path to the upstream\"}}";
    }

    private static String firstValue(Headers h, String name) {
        List<String> l = h.get(name);
        return (l == null || l.isEmpty()) ? null : l.get(0);
    }

    static String jsonEscape(String s) {
        StringBuilder sb = new StringBuilder(s.length() + 8);
        for (char c : s.toCharArray()) {
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        return sb.toString();
    }
}
