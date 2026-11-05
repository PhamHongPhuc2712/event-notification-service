package com.eventnotification.worker.webhook;

import com.eventnotification.worker.TestWorkerProperties;
import com.eventnotification.worker.TestcontainersConfiguration;
import com.eventnotification.worker.delivery.NotificationMessage;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;
import tools.jackson.databind.json.JsonMapper;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Kafka message in, signed HTTP request at the receiver and SENT in Postgres out. */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
@ActiveProfiles("test")
class WebhookDeliveryIntegrationTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static FakeWebhookReceiver receiver;

    @Autowired
    private JdbcClient jdbc;

    @Autowired
    private KafkaTemplate<String, String> kafka;

    @Autowired
    private JsonMapper jsonMapper;

    @BeforeAll
    static void startReceiver() throws Exception {
        receiver = new FakeWebhookReceiver();
    }

    @AfterAll
    static void stopReceiver() {
        receiver.close();
    }

    @Test
    void deliversASignedWebhookAndMarksItSent() throws Exception {
        UUID id = insertWebhook(receiver.url("/orders"));

        publish(id, receiver.url("/orders"));

        await().atMost(TIMEOUT).until(() -> "SENT".equals(statusOf(id)));
        FakeWebhookReceiver.Received request = receiver.receivedFor(id.toString()).getFirst();
        assertThat(request.signature()).isEqualTo(new WebhookSigner(TestWorkerProperties.WEBHOOK_SECRET)
                .sign(request.id(), Long.parseLong(request.timestamp()), request.body()));
    }

    @Test
    void receiverThatRejectsTheRequestMarksItFailed() throws Exception {
        try (FakeWebhookReceiver gone = new FakeWebhookReceiver()) {
            gone.respondWith(410);
            UUID id = insertWebhook(gone.url("/old"));

            publish(id, gone.url("/old"));

            await().atMost(TIMEOUT).until(() -> "FAILED".equals(statusOf(id)));
            assertThat(gone.received()).hasSize(1);
        }
    }

    private UUID insertWebhook(String url) {
        UUID id = UUID.randomUUID();
        Timestamp now = Timestamp.from(Instant.now());
        jdbc.sql("""
                        INSERT INTO notifications
                            (id, user_id, channel, recipient, subject, body, status, created_at, updated_at)
                        VALUES (:id, 'user-1', 'WEBHOOK', :url, 'order.shipped', '{}', 'QUEUED', :now, :now)
                        """)
                .param("id", id)
                .param("url", url)
                .param("now", now)
                .update();
        return id;
    }

    private void publish(UUID id, String url) throws Exception {
        kafka.send("notifications.webhook", "user-1", jsonMapper.writeValueAsString(new NotificationMessage(id,
                "user-1", "WEBHOOK", url, "order.shipped", "{}", Instant.now()))).get();
    }

    private String statusOf(UUID id) {
        return jdbc.sql("SELECT status FROM notifications WHERE id = :id").param("id", id).query(String.class).single();
    }
}
