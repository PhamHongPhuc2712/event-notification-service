package com.eventnotification.worker;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A scriptable SMS provider for worker tests. Each idempotency key answers with the scripted
 * statuses in order, then 202 for every later request.
 */
public class FakeSmsProvider implements AutoCloseable {

    /** Scripted response that answers 202 only after {@link #SLOW_RESPONSE}, past the client's read timeout. */
    public static final int TIMEOUT = -1;
    public static final Duration SLOW_RESPONSE = Duration.ofMillis(1_500);

    private final HttpServer server;
    private final Map<String, Deque<Integer>> scripts = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> attempts = new ConcurrentHashMap<>();
    private volatile Integer alwaysRespond;
    private volatile Duration latency = Duration.ZERO;

    public FakeSmsProvider() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        server.createContext("/v1/messages", this::handle);
        server.start();
    }

    public URI baseUrl() {
        return URI.create("http://127.0.0.1:" + server.getAddress().getPort());
    }

    /** Answers the next requests for this key with these statuses, in order. */
    public void script(String key, Integer... statuses) {
        scripts.put(key, new ArrayDeque<>(List.of(statuses)));
    }

    /** Answers every request with this status until reset with null. */
    public void respondAlways(Integer status) {
        alwaysRespond = status;
    }

    /** Adds this delay before every response, like a provider under load. */
    public void setLatency(Duration latency) {
        this.latency = latency;
    }

    public int attempts(String key) {
        AtomicInteger count = attempts.get(key);
        return count == null ? 0 : count.get();
    }

    private void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            exchange.getRequestBody().readAllBytes();
            String key = exchange.getRequestHeaders().getFirst("Idempotency-Key");
            attempts.computeIfAbsent(String.valueOf(key), k -> new AtomicInteger()).incrementAndGet();
            int status = nextStatus(key);
            if (status == TIMEOUT) {
                sleep(SLOW_RESPONSE);
                status = 202;
            } else if (!latency.isZero()) {
                sleep(latency);
            }
            byte[] body = "{\"status\":\"accepted\"}".getBytes();
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(status, body.length);
            exchange.getResponseBody().write(body);
        }
    }

    private int nextStatus(String key) {
        Integer always = alwaysRespond;
        if (always != null) {
            return always;
        }
        Deque<Integer> script = key == null ? null : scripts.get(key);
        Integer next = script == null ? null : script.poll();
        return next == null ? 202 : next;
    }

    private static void sleep(Duration duration) {
        try {
            Thread.sleep(duration);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
