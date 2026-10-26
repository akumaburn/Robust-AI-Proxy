package com.robust.aiproxy;

import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.concurrent.Executors;

/** Owns the HTTP server, the shared upstream HttpClient, and the handler. */
public final class ProxyServer {

    private final Config config;
    private final HttpClient http;
    private HttpServer server;

    public ProxyServer(Config config) {
        this.config = config;
        this.http = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_2)
                .connectTimeout(Duration.ofSeconds(10))
                .followRedirects(HttpClient.Redirect.NEVER)
                .build();
    }

    public HttpServer start() {
        try {
            server = HttpServer.create(new InetSocketAddress(config.bind, config.port), 64);
        } catch (IOException e) {
            throw new IllegalStateException(
                    "cannot bind " + config.bind + ":" + config.port + " - " + e.getMessage(), e);
        }
        server.setExecutor(Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "rai-client-handler");
            t.setDaemon(true);
            return t;
        }));
        server.createContext("/", new ProxyHandler(config, http));
        server.start();
        return server;
    }
}
