package com.robust.aiproxy;

import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.URI;
import java.net.UnknownHostException;
import java.net.http.HttpClient;
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.net.http.HttpTimeoutException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SSLException;

/**
 * Executes one upstream attempt with an idle watchdog.
 *
 * A worker thread reads the upstream response into a queue; the driver
 * (the request-handling thread) polls that queue with a timeout equal to
 * the idle limit. When the queue stays empty for the idle limit, the
 * attempt is aborted (connection cancelled, stream closed) and reported as
 * IDLE_TIMEOUT so the caller can retry. Any optionally-configured total
 * timeout is enforced on the same poll loop.
 */
public final class UpstreamClient {

    /** Items flowing from the worker thread to the attempt driver. */
    public sealed interface Item permits Meta, Chunk, Eof, Fail {
    }

    /** Upstream response headers arrived. */
    public record Meta(int status, Map<String, List<String>> headers) implements Item {
    }

    /** A piece of response body. */
    public record Chunk(byte[] bytes) implements Item {
    }

    /** Upstream closed the stream cleanly. */
    public record Eof() implements Item {
    }

    /** Worker thread hit an error. */
    public record Fail(String reason, Throwable cause) implements Item {
    }

    public enum Kind {
        COMPLETED, IDLE_TIMEOUT, TOTAL_TIMEOUT, TRANSPORT_ERROR, CLIENT_GONE
    }

    public record AttemptOutcome(Kind kind, int status, Map<String, List<String>> headers, String failReason) {
    }

    /** Callback receiving delivered items. */
    public interface ItemSink {
        void onMeta(int status, Map<String, List<String>> headers);

        void onChunk(byte[] bytes);

        /** True once the sink can no longer accept data (e.g. client gone). */
        default boolean clientGone() {
            return false;
        }
    }

    private static final int READ_BUF_SIZE = 8192;

    private final HttpClient http;

    public UpstreamClient(HttpClient http) {
        this.http = http;
    }

    /**
     * Runs one attempt, delivering items to the sink until the attempt
     * completes or fails. Never throws; the outcome is always returned.
     */
    public AttemptOutcome attempt(Config cfg, String method, URI uri,
                                   List<Map.Entry<String, String>> headers, byte[] body,
                                   ItemSink sink) {
        BlockingQueue<Item> queue = new LinkedBlockingQueue<>();
        AtomicBoolean aborted = new AtomicBoolean();
        AtomicReference<InputStream> streamRef = new AtomicReference<>();
        long t0 = System.nanoTime();

        HttpRequest request = buildRequest(method, uri, headers, body);
        CompletableFuture<HttpResponse<InputStream>> send =
                http.sendAsync(request, BodyHandlers.ofInputStream());

        Thread worker = new Thread(() -> {
            try {
                HttpResponse<InputStream> resp = send.join();
                if (aborted.get()) {
                    return;
                }
                queue.add(new Meta(resp.statusCode(), resp.headers().map()));
                InputStream is = resp.body();
                streamRef.set(is);
                byte[] buf = new byte[READ_BUF_SIZE];
                int n;
                while ((n = is.read(buf)) != -1) {
                    if (aborted.get()) {
                        return;
                    }
                    queue.add(new Chunk(Arrays.copyOf(buf, n)));
                }
                queue.add(new Eof());
            } catch (CancellationException cex) {
                // driver aborted this attempt; nothing to report
            } catch (Throwable t) {
                if (!aborted.get()) {
                    queue.add(new Fail(describe(t), t));
                }
            }
        }, "rai-upstream-reader");
        worker.setDaemon(true);
        worker.start();

        long totalDeadlineNanos = cfg.totalTimeoutMs > 0
                ? t0 + cfg.totalTimeoutMs * 1_000_000L
                : 0L;
        int status = 0;
        Map<String, List<String>> respHeaders = Map.of();

        while (true) {
            long pollMs = cfg.idleTimeoutMs;
            if (totalDeadlineNanos > 0) {
                long remainingMs = (totalDeadlineNanos - System.nanoTime()) / 1_000_000L;
                if (remainingMs <= 0) {
                    abort(aborted, send, streamRef, worker);
                    return new AttemptOutcome(Kind.TOTAL_TIMEOUT, status, respHeaders, "total timeout exceeded");
                }
                pollMs = Math.min(pollMs, remainingMs);
            }
            Item item;
            try {
                item = queue.poll(pollMs, TimeUnit.MILLISECONDS);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                abort(aborted, send, streamRef, worker);
                return new AttemptOutcome(Kind.TRANSPORT_ERROR, status, respHeaders, "interrupted");
            }
            if (item == null) {
                boolean totalFired = totalDeadlineNanos > 0 && System.nanoTime() >= totalDeadlineNanos;
                abort(aborted, send, streamRef, worker);
                String reason = totalFired
                        ? "total timeout exceeded"
                        : "idle timeout: no upstream bytes for " + (cfg.idleTimeoutMs / 1000) + "s";
                return new AttemptOutcome(totalFired ? Kind.TOTAL_TIMEOUT : Kind.IDLE_TIMEOUT,
                        status, respHeaders, reason);
            }
            if (item instanceof Meta m) {
                status = m.status();
                respHeaders = m.headers();
                Log.info("upstream: status=%d after %.2fs", status, (System.nanoTime() - t0) / 1e9);
                sink.onMeta(status, respHeaders);
            } else if (item instanceof Chunk c) {
                sink.onChunk(c.bytes());
                if (sink.clientGone()) {
                    abort(aborted, send, streamRef, worker);
                    return new AttemptOutcome(Kind.CLIENT_GONE, status, respHeaders, "client disconnected");
                }
            } else if (item instanceof Eof) {
                joinWorkerQuietly(worker);
                return new AttemptOutcome(Kind.COMPLETED, status, respHeaders, null);
            } else if (item instanceof Fail f) {
                joinWorkerQuietly(worker);
                return new AttemptOutcome(Kind.TRANSPORT_ERROR, status, respHeaders, f.reason());
            }
        }
    }

    private static HttpRequest buildRequest(String method, URI uri,
                                           List<Map.Entry<String, String>> headers, byte[] body) {
        HttpRequest.BodyPublisher publisher = (body == null || body.length == 0)
                ? BodyPublishers.noBody()
                : BodyPublishers.ofByteArray(body);
        HttpRequest.Builder b = HttpRequest.newBuilder(uri).method(method, publisher);
        for (Map.Entry<String, String> h : headers) {
            b.header(h.getKey(), h.getValue());
        }
        return b.build();
    }

    private static void abort(AtomicBoolean aborted,
                              CompletableFuture<HttpResponse<InputStream>> send,
                              AtomicReference<InputStream> streamRef,
                              Thread worker) {
        aborted.set(true);
        send.cancel(true);
        InputStream is = streamRef.get();
        if (is != null) {
            try {
                is.close();
            } catch (IOException ignored) {
            }
        }
        try {
            worker.join(3000);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    private static void joinWorkerQuietly(Thread worker) {
        try {
            worker.join(2000);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    private static String describe(Throwable t) {
        Throwable cur = t;
        while (cur != null) {
            if (cur instanceof HttpConnectTimeoutException) {
                return "upstream connect timeout";
            }
            if (cur instanceof HttpTimeoutException) {
                return "upstream request timeout";
            }
            if (cur instanceof ConnectException) {
                return "upstream connection refused: " + safeMessage(cur);
            }
            if (cur instanceof UnknownHostException) {
                return "upstream host not resolvable: " + safeMessage(cur);
            }
            if (cur instanceof SSLException) {
                return "upstream TLS error: " + safeMessage(cur);
            }
            cur = cur.getCause();
        }
        String msg = t.getMessage();
        return t.getClass().getSimpleName() + (msg == null ? "" : ": " + msg);
    }

    private static String safeMessage(Throwable t) {
        String m = t.getMessage();
        return m == null ? t.getClass().getSimpleName() : m;
    }
}
