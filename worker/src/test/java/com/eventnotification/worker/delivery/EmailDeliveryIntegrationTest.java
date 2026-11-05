package com.eventnotification.worker.delivery;

import com.eventnotification.worker.TestcontainersConfiguration;
import com.eventnotification.worker.TopicReader;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.kafka.KafkaContainer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Kafka message in, email in Mailpit and SENT status in Postgres out. */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
@ActiveProfiles("test")
class EmailDeliveryIntegrationTest {

    private static final String EMAIL_TOPIC = "notifications.email";
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @Autowired
    private JdbcClient jdbc;

    @Autowired
    private KafkaTemplate<String, String> kafka;

    @Autowired
    private GenericContainer<?> mailpit;

    @Autowired
    private KafkaContainer kafkaContainer;

    @Autowired
    private JsonMapper jsonMapper;

    private final HttpClient http = HttpClient.newHttpClient();

    @Test
    void deliversQueuedNotificationAndMarksItSent() throws Exception {
        String userId = uniqueUserId();
        String subject = "Order shipped " + UUID.randomUUID();
        UUID id = insertNotification(userId, subject, "QUEUED");

        publish(userId, message(id, userId, subject));

        await().atMost(TIMEOUT).until(() -> "SENT".equals(statusOf(id)));
        assertThat(jdbc.sql("SELECT sent_at IS NOT NULL FROM notifications WHERE id = :id")
                .param("id", id).query(Boolean.class).single()).isTrue();
        await().atMost(TIMEOUT).until(() -> mailpitSubjects().contains(subject));
    }

    @Test
    void doesNotResendNotificationThatWasAlreadySent() throws Exception {
        String userId = uniqueUserId();
        String alreadySentSubject = "Already sent " + UUID.randomUUID();
        String nextSubject = "Next " + UUID.randomUUID();
        UUID alreadySent = insertNotification(userId, alreadySentSubject, "SENT");
        UUID next = insertNotification(userId, nextSubject, "QUEUED");

        // Same key, so same partition: "next" is processed only after the duplicate.
        publish(userId, message(alreadySent, userId, alreadySentSubject));
        publish(userId, message(next, userId, nextSubject));

        await().atMost(TIMEOUT).until(() -> mailpitSubjects().contains(nextSubject));
        assertThat(mailpitSubjects()).doesNotContain(alreadySentSubject);
    }

    @Test
    void deadLettersMalformedMessageAndKeepsConsuming() throws Exception {
        String userId = uniqueUserId();
        String subject = "After poison " + UUID.randomUUID();
        UUID id = insertNotification(userId, subject, "QUEUED");
        String poison = "{this is not json " + UUID.randomUUID();

        publish(userId, poison);
        publish(userId, message(id, userId, subject));

        await().atMost(TIMEOUT).until(() -> "SENT".equals(statusOf(id)));
        TopicReader.awaitRecord(kafkaContainer.getBootstrapServers(), EMAIL_TOPIC + "-dlt",
                r -> poison.equals(r.value()));
    }

    @Test
    void skipsEmptyAndNullMessagesWithoutStallingThePartition() throws Exception {
        String userId = uniqueUserId();
        String subject = "After empty " + UUID.randomUUID();
        UUID id = insertNotification(userId, subject, "QUEUED");

        publish(userId, null);
        publish(userId, "null");
        publish(userId, message(id, userId, subject));

        await().atMost(TIMEOUT).until(() -> "SENT".equals(statusOf(id)));
    }

    @Test
    void suppressesEmailWhenTheUserTurnedEmailOff() throws Exception {
        String userId = uniqueUserId();
        String subject = "Suppressed " + UUID.randomUUID();
        UUID id = insertNotification(userId, subject, "QUEUED");
        savePreferences(userId, "{SMS}", null, null, null);

        publish(userId, message(id, userId, subject));

        await().atMost(TIMEOUT).until(() -> "SUPPRESSED".equals(statusOf(id)));
        assertThat(jdbc.sql("SELECT failure_reason FROM notifications WHERE id = :id")
                .param("id", id).query(String.class).single()).isEqualTo("CHANNEL_DISABLED");
        assertThat(mailpitSubjects()).doesNotContain(subject);
    }

    @Test
    void defersEmailDuringQuietHoursUntilTheyEnd() throws Exception {
        String userId = uniqueUserId();
        String subject = "Deferred " + UUID.randomUUID();
        UUID id = insertNotification(userId, subject, "QUEUED");
        // A window from one hour ago to one hour from now, in UTC, is quiet right now.
        LocalTime now = LocalTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.MINUTES);
        savePreferences(userId, "{EMAIL}", now.minusHours(1), now.plusHours(1), "UTC");

        publish(userId, message(id, userId, subject));

        await().atMost(TIMEOUT).until(() -> "DEFERRED".equals(statusOf(id)));
        Instant deliverAfter = jdbc.sql("SELECT deliver_after FROM notifications WHERE id = :id")
                .param("id", id).query(Timestamp.class).single().toInstant();
        assertThat(Duration.between(Instant.now(), deliverAfter)).isBetween(Duration.ofMinutes(58),
                Duration.ofMinutes(61));
        assertThat(mailpitSubjects()).doesNotContain(subject);
    }

    private void savePreferences(String userId, String channels, LocalTime start, LocalTime end, String zone) {
        jdbc.sql("""
                        INSERT INTO user_preferences
                            (user_id, allowed_channels, quiet_start, quiet_end, time_zone, updated_at)
                        VALUES (:userId, CAST(:channels AS TEXT[]), :start, :end, :zone, now())
                        """)
                .param("userId", userId)
                .param("channels", channels)
                .param("start", start == null ? null : Time.valueOf(start))
                .param("end", end == null ? null : Time.valueOf(end))
                .param("zone", zone)
                .update();
    }

    private UUID insertNotification(String userId, String subject, String status) {
        UUID id = UUID.randomUUID();
        Timestamp now = Timestamp.from(Instant.now());
        jdbc.sql("""
                        INSERT INTO notifications
                            (id, user_id, channel, recipient, subject, body, status, created_at, updated_at)
                        VALUES (:id, :userId, 'EMAIL', 'bob@example.com', :subject, 'Body', :status, :now, :now)
                        """)
                .param("id", id)
                .param("userId", userId)
                .param("subject", subject)
                .param("status", status)
                .param("now", now)
                .update();
        return id;
    }

    private String message(UUID id, String userId, String subject) {
        return jsonMapper.writeValueAsString(new NotificationMessage(id, userId, "EMAIL", "bob@example.com",
                subject, "Body", Instant.now()));
    }

    private void publish(String key, String value) throws Exception {
        kafka.send(EMAIL_TOPIC, key, value).get();
    }

    private String statusOf(UUID id) {
        return jdbc.sql("SELECT status FROM notifications WHERE id = :id").param("id", id).query(String.class).single();
    }

    private List<String> mailpitSubjects() throws Exception {
        URI uri = URI.create("http://" + mailpit.getHost() + ":"
                + mailpit.getMappedPort(TestcontainersConfiguration.MAILPIT_HTTP_PORT) + "/api/v1/messages?limit=1000");
        HttpResponse<String> response = http.send(HttpRequest.newBuilder(uri).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        List<String> subjects = new ArrayList<>();
        for (JsonNode m : jsonMapper.readTree(response.body()).get("messages")) {
            subjects.add(m.get("Subject").asString());
        }
        return subjects;
    }

    private static String uniqueUserId() {
        return "user-" + UUID.randomUUID().toString().substring(0, 8);
    }
}
