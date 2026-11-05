package com.eventnotification.chaos;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Kills workers mid-run and checks the run ends with zero lost and zero duplicate deliveries.
 *
 * <p>Runs the real jars: mock-sms with fault injection, the api, and three workers, against
 * Postgres and Kafka in containers. Clients send SMS notifications at a steady rate, retrying
 * with the same Idempotency-Key whenever the API is unreachable. Meanwhile workers are killed
 * with the equivalent of {@code kill -9} and replaced, and the API is killed once too.
 *
 * <p>Each of the 50 users receives 20 SMS against a limit of 5 a minute, so most notifications
 * are deferred to a reserved rate-limit slot and released later by the API.
 */
@Testcontainers
class ChaosTest {

    private static final int NOTIFICATIONS = 1_000;
    private static final int CLIENTS = 8;
    private static final Duration CLIENT_PAUSE = Duration.ofMillis(400); // about 20 requests a second
    private static final int WORKERS = 3;
    private static final Duration CHAOS_INTERVAL = Duration.ofSeconds(7);
    private static final Duration STARTUP_TIMEOUT = Duration.ofSeconds(90);
    // Rate limits spread each user's 20 SMS over about four minutes.
    private static final Duration DRAIN_TIMEOUT = Duration.ofMinutes(8);

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16.15-alpine");

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.3.2");

    @Container
    static final GenericContainer<?> REDIS = new GenericContainer<>("redis:7.4.12-alpine").withExposedPorts(6379);

    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
    private final JsonMapper json = JsonMapper.builder().build();
    private final List<ServiceProcess> running = new ArrayList<>();
    private final AtomicInteger instances = new AtomicInteger();
    private final List<String> chaosLog = new ArrayList<>();

    @AfterEach
    void stopEverything() throws InterruptedException {
        for (ServiceProcess p : running) {
            p.kill();
        }
    }

    @Test
    void killingWorkersAndTheApiMidRunLosesAndDuplicatesNothing() throws Exception {
        ServiceProcess mockSms = start("mock-sms", List.of(
                "--mock-sms.faults.failure-rate=0.1",
                "--mock-sms.faults.timeout-rate=0.05",
                "--mock-sms.faults.slow-rate=0.1",
                "--mock-sms.faults.timeout-delay=4s",
                "--mock-sms.faults.slow-delay=500ms")).awaitHealthy(STARTUP_TIMEOUT);
        AtomicReference<ServiceProcess> api = new AtomicReference<>(startApi().awaitHealthy(STARTUP_TIMEOUT));
        List<ServiceProcess> workers = new ArrayList<>();
        for (int i = 0; i < WORKERS; i++) {
            workers.add(startWorker(mockSms));
        }
        for (ServiceProcess worker : workers) {
            worker.awaitHealthy(STARTUP_TIMEOUT);
        }

        Instant started = Instant.now();
        Map<String, UUID> idsByKey = new ConcurrentHashMap<>();
        AtomicInteger clientRetries = new AtomicInteger();
        try (ExecutorService clients = Executors.newFixedThreadPool(CLIENTS)) {
            List<Future<?>> sends = new ArrayList<>();
            AtomicInteger next = new AtomicInteger();
            for (int c = 0; c < CLIENTS; c++) {
                sends.add(clients.submit(() -> {
                    for (int i = next.getAndIncrement(); i < NOTIFICATIONS; i = next.getAndIncrement()) {
                        String key = "chaos-" + i;
                        idsByKey.put(key, sendUntilAccepted(api, key, i, clientRetries));
                        Thread.sleep(CLIENT_PAUSE);
                    }
                    return null;
                }));
            }

            int round = 0;
            while (!allDone(sends)) {
                Thread.sleep(CHAOS_INTERVAL);
                round++;
                if (round == 2) {
                    api.get().kill();
                    chaosLog.add(elapsed(started) + " killed " + api.get().name());
                    api.set(startApi().awaitHealthy(STARTUP_TIMEOUT));
                    chaosLog.add(elapsed(started) + " " + api.get().name() + " healthy");
                } else {
                    int victims = round % 3 == 0 ? 2 : 1;
                    for (int v = 0; v < victims; v++) {
                        ServiceProcess victim = workers.remove(ThreadLocalRandom.current().nextInt(workers.size()));
                        victim.kill();
                        chaosLog.add(elapsed(started) + " killed " + victim.name());
                        workers.add(startWorker(mockSms));
                    }
                }
            }
            for (Future<?> send : sends) {
                send.get();
            }
        }
        chaosLog.add(elapsed(started) + " all " + NOTIFICATIONS + " notifications accepted by the API");

        long rateLimited = countWhere("rate_limit_reserved");
        await().atMost(DRAIN_TIMEOUT).pollInterval(Duration.ofSeconds(2))
                .until(() -> countWhere("status IN ('QUEUED', 'DEFERRED')") == 0);
        chaosLog.add(elapsed(started) + " no notification left QUEUED or DEFERRED");
        rateLimited = Math.max(rateLimited, countWhere("rate_limit_reserved"));

        Map<String, Long> statuses = statusCounts();
        Set<UUID> sentIds = sentIds();
        Set<String> providerKeys = providerKeys(mockSms);
        JsonNode providerStats = getJson(mockSms.baseUrl().resolve("/v1/messages/stats"));
        writeReport(statuses, providerStats, clientRetries.get(), rateLimited,
                Duration.between(started, Instant.now()));

        // One notification per Idempotency-Key, despite client retries while the API was down.
        assertThat(new HashSet<>(idsByKey.values())).hasSize(NOTIFICATIONS);
        assertThat(countWhere("TRUE")).isEqualTo(NOTIFICATIONS);
        // Zero lost: every notification was delivered to the provider and marked SENT.
        assertThat(statuses).containsOnlyKeys("SENT");
        assertThat(providerKeys).containsExactlyInAnyOrderElementsOf(
                sentIds.stream().map(UUID::toString).toList());
        // Zero duplicates: the provider accepted exactly one SMS per notification.
        assertThat(providerStats.get("accepted").asLong()).isEqualTo(NOTIFICATIONS);
    }

    private UUID sendUntilAccepted(AtomicReference<ServiceProcess> api, String key, int i, AtomicInteger retries)
            throws InterruptedException {
        String body = """
                {"userId":"chaos-user-%d","channel":"SMS","recipient":"+6591%06d","body":"Chaos message %d"}"""
                .formatted(i % 50, i, i);
        while (true) {
            try {
                HttpRequest request = HttpRequest.newBuilder(api.get().baseUrl().resolve("/notifications"))
                        .timeout(Duration.ofSeconds(5))
                        .header("Content-Type", "application/json")
                        .header("Idempotency-Key", key)
                        .POST(HttpRequest.BodyPublishers.ofString(body))
                        .build();
                HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() == 202) {
                    return UUID.fromString(json.readTree(response.body()).get("id").asString());
                }
                if (response.statusCode() < 500) {
                    throw new IllegalStateException("API rejected " + key + ": " + response.body());
                }
            } catch (IOException e) {
                // API down or restarting: retry with the same key.
            }
            retries.incrementAndGet();
            Thread.sleep(250);
        }
    }

    private ServiceProcess startApi() throws IOException {
        return start("api", List.of(
                "--spring.datasource.url=" + POSTGRES.getJdbcUrl(),
                "--spring.datasource.username=" + POSTGRES.getUsername(),
                "--spring.datasource.password=" + POSTGRES.getPassword(),
                "--spring.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(),
                "--notifications.deferred.poll-interval=1s"));
    }

    private ServiceProcess startWorker(ServiceProcess mockSms) throws IOException {
        return start("worker", List.of(
                "--spring.datasource.url=" + POSTGRES.getJdbcUrl(),
                "--spring.datasource.username=" + POSTGRES.getUsername(),
                "--spring.datasource.password=" + POSTGRES.getPassword(),
                "--spring.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(),
                // Real rate limits: each user gets 20 SMS, so most wait for a reserved slot.
                "--spring.data.redis.host=" + REDIS.getHost(),
                "--spring.data.redis.port=" + REDIS.getMappedPort(6379),
                // A dead worker's partitions move after 6s, and its claims expire after 10s.
                "--spring.kafka.consumer.properties.session.timeout.ms=6000",
                "--spring.kafka.consumer.properties.heartbeat.interval.ms=2000",
                // A test-only secret; this run sends no webhooks, but workers need one to start.
                "--notifications.webhook.signing-secret=whsec_gFEooYrtFboec/3WTK9KEtPSZFiJhs36b+clSo+96IA=",
                "--notifications.delivery.claim-lease=10s",
                "--notifications.sms.base-url=" + mockSms.baseUrl(),
                "--notifications.sms.read-timeout=2s",
                // 10 attempts over about 32s: longer than the claim lease, so a retry can take a
                // dead worker's claim, and a 15% fault rate almost never exhausts them.
                "--notifications.retry.attempts=10",
                "--notifications.retry.initial-delay=500ms",
                "--notifications.retry.multiplier=2",
                "--notifications.retry.max-delay=5s",
                "--notifications.retry.jitter=200ms"));
    }

    private ServiceProcess start(String module, List<String> args) throws IOException {
        ServiceProcess process = ServiceProcess.start(module, module + "-" + instances.incrementAndGet(), args);
        running.add(process);
        return process;
    }

    private static boolean allDone(List<Future<?>> futures) {
        return futures.stream().allMatch(Future::isDone);
    }

    private static String elapsed(Instant started) {
        return "t+" + Duration.between(started, Instant.now()).toSeconds() + "s";
    }

    private Connection db() throws SQLException {
        return DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
    }

    /** {@code condition} is a constant from this class, never input. */
    private long countWhere(String condition) throws SQLException {
        try (Connection c = db(); ResultSet rs = c.createStatement()
                .executeQuery("SELECT count(*) FROM notifications WHERE " + condition)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private Map<String, Long> statusCounts() throws SQLException {
        Map<String, Long> counts = new TreeMap<>();
        try (Connection c = db(); ResultSet rs = c.createStatement()
                .executeQuery("SELECT status, count(*) FROM notifications GROUP BY status")) {
            while (rs.next()) {
                counts.put(rs.getString(1), rs.getLong(2));
            }
        }
        return counts;
    }

    private Set<UUID> sentIds() throws SQLException {
        Set<UUID> ids = new HashSet<>();
        try (Connection c = db(); ResultSet rs = c.createStatement()
                .executeQuery("SELECT id FROM notifications WHERE status = 'SENT'")) {
            while (rs.next()) {
                ids.add(rs.getObject(1, UUID.class));
            }
        }
        return ids;
    }

    private Set<String> providerKeys(ServiceProcess mockSms) throws IOException, InterruptedException {
        Set<String> keys = new HashSet<>();
        for (JsonNode message : getJson(mockSms.baseUrl().resolve("/v1/messages"))) {
            keys.add(message.get("idempotencyKey").asString());
        }
        return keys;
    }

    private JsonNode getJson(URI uri) throws IOException, InterruptedException {
        HttpResponse<String> response = http.send(HttpRequest.newBuilder(uri).build(),
                HttpResponse.BodyHandlers.ofString());
        return json.readTree(response.body());
    }

    private void writeReport(Map<String, Long> statuses, JsonNode provider, int clientRetries, long rateLimited,
                             Duration total)
            throws IOException {
        String report = """
                # Chaos test report

                %d SMS notifications, %d clients, %d workers. Total run time %ds.

                ## What was broken

                %s

                The SMS provider injected %d failures (503), %d timeouts (accepted, but answered after the
                worker gave up) and %d slow responses.

                ## Result

                | Measure | Value |
                |---|---|
                | Notifications created | %s |
                | Final statuses | %s |
                | SMS accepted by the provider | %d |
                | Provider requests in total | %d |
                | Repeat requests absorbed by the provider's idempotency key | %d |
                | Client retries while the API was down | %d |
                | Deferred to a rate-limit slot at least once | %d |
                """.formatted(NOTIFICATIONS, CLIENTS, WORKERS, total.toSeconds(),
                String.join("\n", chaosLog.stream().map(line -> "- " + line).toList()),
                provider.get("injectedFailures").asLong(), provider.get("injectedTimeouts").asLong(),
                provider.get("injectedSlowResponses").asLong(),
                statuses.values().stream().mapToLong(Long::longValue).sum(), statuses,
                provider.get("accepted").asLong(), provider.get("requests").asLong(),
                provider.get("duplicateRequests").asLong(), clientRetries, rateLimited);
        Files.writeString(Path.of("target", "chaos-report.md"), report);
        System.out.println(report);
    }
}
