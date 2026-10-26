package com.robust.aiproxy;

import com.sun.net.httpserver.HttpServer;

/**
 * Robust-AI-Proxy entry point.
 *
 * A small OpenAI-compatible reverse proxy that forwards requests to an LLM
 * provider, watches the upstream stream for stalls, and transparently
 * retries the upstream request when no bytes arrive for the idle timeout.
 * Defaults to listening on port 8000 of the current machine.
 */
public final class Main {

    public static final String VERSION = "1.0.0";

    private Main() {
    }

    public static void main(String[] args) {
        Config config;
        try {
            config = Config.fromSystem();
        } catch (IllegalStateException e) {
            System.err.println("config error: " + e.getMessage());
            System.exit(1);
            return;
        }

        Log.info("Robust-AI-Proxy v" + VERSION + " starting");
        Log.info("config: %s", Config.describe(config));

        HttpServer server = new ProxyServer(config).start();
        String bindDesc = "0.0.0.0".equals(config.bind) ? "0.0.0.0 (all interfaces)" : config.bind;
        Log.info("listening on %s:%d", bindDesc, config.port);
        Log.info("forwarding to %s", config.upstream);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            Log.info("stopping (shutdown signal)");
            server.stop(1);
        }, "rai-shutdown"));
    }
}
