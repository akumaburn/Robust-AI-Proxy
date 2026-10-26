package com.robust.aiproxy;

import java.util.Locale;

/**
 * Effective proxy configuration.
 *
 * Precedence: system property (-Dname) &gt; environment variable (RAI_PROXY_*) &gt; default.
 * Defaults: port 8000, bind 0.0.0.0, upstream https://api.openai.com/v1,
 * idle timeout 300 s, total timeout off, unlimited retries, buffer mode.
 */
public final class Config {

    public final int port;
    public final String bind;
    public final String upstream;
    public final String apiKey;
    public final long idleTimeoutMs;
    public final long totalTimeoutMs; // 0 = no total timeout
    public final int maxRetries;      // -1 = unlimited, 0 = none, N = retries after the first attempt
    public final long retryBackoffMs;
    public final String streamMode;   // "buffer" or "passthrough"
    public final boolean requireDone; // require a terminal "data: [DONE]" for SSE responses

    private Config(int port, String bind, String upstream, String apiKey,
                   long idleTimeoutMs, long totalTimeoutMs, int maxRetries,
                   long retryBackoffMs, String streamMode, boolean requireDone) {
        this.port = port;
        this.bind = bind;
        this.upstream = upstream;
        this.apiKey = apiKey;
        this.idleTimeoutMs = idleTimeoutMs;
        this.totalTimeoutMs = totalTimeoutMs;
        this.maxRetries = maxRetries;
        this.retryBackoffMs = retryBackoffMs;
        this.streamMode = streamMode;
        this.requireDone = requireDone;
    }

    public static Config fromSystem() {
        int port = intProp("port", "RAI_PROXY_PORT", 8000);
        String bind = strProp("bind", "RAI_PROXY_BIND", "0.0.0.0");
        String upstream = strProp("upstream", "RAI_PROXY_UPSTREAM", "https://api.openai.com/v1");
        String apiKey = strProp("apiKey", "RAI_PROXY_API_KEY", "");
        long idle = longProp("idleTimeoutSeconds", "RAI_PROXY_IDLE_TIMEOUT_SECONDS", 300);
        long total = longProp("totalTimeoutSeconds", "RAI_PROXY_TOTAL_TIMEOUT_SECONDS", 0);
        int maxRetries = intProp("maxRetries", "RAI_PROXY_MAX_RETRIES", -1);
        long backoff = longProp("retryBackoffMs", "RAI_PROXY_RETRY_BACKOFF_MS", 500);
        String mode = strProp("streamMode", "RAI_PROXY_STREAM_MODE", "buffer").toLowerCase(Locale.ROOT);
        boolean requireDone = boolProp("requireDone", "RAI_PROXY_REQUIRE_DONE", true);

        if (port < 1 || port > 65535) {
            throw new IllegalStateException("port out of range: " + port);
        }
        if (idle < 1) {
            throw new IllegalStateException("idle timeout must be at least 1 second");
        }
        if (total < 0) {
            throw new IllegalStateException("total timeout must be >= 0 (0 disables it)");
        }
        if (backoff < 0) {
            throw new IllegalStateException("retry backoff must be >= 0");
        }
        if (!mode.equals("buffer") && !mode.equals("passthrough")) {
            throw new IllegalStateException("streamMode must be 'buffer' or 'passthrough', got: " + mode);
        }

        return new Config(port, bind, stripTrailingSlash(upstream), apiKey,
                idle * 1000, total * 1000, maxRetries, backoff, mode, requireDone);
    }

    /** One-line summary for the startup log. The API key is never printed. */
    public static String describe(Config c) {
        return String.format(
                "port=%d bind=%s upstream=%s mode=%s idle=%ss total=%s maxRetries=%s backoff=%dms requireDone=%b apiKey=%s",
                c.port, c.bind, c.upstream, c.streamMode, c.idleTimeoutMs / 1000,
                c.totalTimeoutMs == 0 ? "off" : (c.totalTimeoutMs / 1000) + "s",
                c.maxRetries < 0 ? "unlimited" : String.valueOf(c.maxRetries),
                c.retryBackoffMs, c.requireDone, c.apiKey.isEmpty() ? "<unset>" : "<set>");
    }

    private static String stripTrailingSlash(String s) {
        while (s.endsWith("/")) {
            s = s.substring(0, s.length() - 1);
        }
        return s;
    }

    private static String strProp(String sysKey, String envKey, String def) {
        String v = System.getProperty(sysKey);
        if (v == null || v.isBlank()) {
            v = System.getenv(envKey);
        }
        if (v == null || v.isBlank()) {
            v = def;
        }
        return v.trim();
    }

    private static int intProp(String sysKey, String envKey, int def) {
        try {
            return Integer.parseInt(strProp(sysKey, envKey, String.valueOf(def)));
        } catch (NumberFormatException e) {
            throw new IllegalStateException("invalid integer for " + sysKey + ": " + strProp(sysKey, envKey, ""));
        }
    }

    private static long longProp(String sysKey, String envKey, long def) {
        try {
            return Long.parseLong(strProp(sysKey, envKey, String.valueOf(def)));
        } catch (NumberFormatException e) {
            throw new IllegalStateException("invalid number for " + sysKey + ": " + strProp(sysKey, envKey, ""));
        }
    }

    private static boolean boolProp(String sysKey, String envKey, boolean def) {
        return Boolean.parseBoolean(strProp(sysKey, envKey, String.valueOf(def)));
    }
}
