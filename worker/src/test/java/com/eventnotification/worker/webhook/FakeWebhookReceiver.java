package com.eventnotification.worker.webhook;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;

/** Records every webhook it receives and answers with a configurable status. */
class FakeWebhookReceiver implements AutoCloseable {

    record Received(String id, String timestamp, String signature, String body) {
    }

    private final HttpServer server;
    private final List<Received> received = new CopyOnWriteArrayList<>();
    private volatile int status = 200;
    private volatile Duration delay = Duration.ZERO;
    private volatile Map<String, String> extraHeaders = Map.of();

    FakeWebhookReceiver() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        server.createContext("/", this::handle);
        server.start();
    }

    String url(String path) {
        return "http://127.0.0.1:" + server.getAddress().getPort() + path;
    }

    void respondWith(int status) {
        this.status = status;
    }

    void respondWith(int status, Map<String, String> headers) {
        this.status = status;
        this.extraHeaders = headers;
    }

    void delay(Duration delay) {
        this.delay = delay;
    }

    List<Received> received() {
        return received;
    }

    List<Received> receivedFor(String id) {
        return received.stream().filter(r -> id.equals(r.id())).toList();
    }

    private void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            received.add(new Received(exchange.getRequestHeaders().getFirst("webhook-id"),
                    exchange.getRequestHeaders().getFirst("webhook-timestamp"),
                    exchange.getRequestHeaders().getFirst("webhook-signature"), body));
            if (!delay.isZero()) {
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            extraHeaders.forEach((k, v) -> exchange.getResponseHeaders().add(k, v));
            exchange.sendResponseHeaders(status, -1);
        }
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
