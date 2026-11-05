package com.eventnotification.worker.ratelimit;

import com.eventnotification.worker.FakeSmsProvider;
import com.eventnotification.worker.TestcontainersConfiguration;
import com.eventnotification.worker.delivery.NotificationMessage;
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
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** A user over their SMS limit is deferred to a reserved slot, then sent without taking another token. */
@SpringBootTest(properties = {
        "notifications.rate-limit.channels.SMS.capacity=2",
        "notifications.rate-limit.channels.SMS.refill-period=1h"
})
@Import(TestcontainersConfiguration.class)
@ActiveProfiles("test")
class SmsRateLimitIntegrationTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @Autowired
    private JdbcClient jdbc;

    @Autowired
    private KafkaTemplate<String, String> kafka;

    @Autowired
    private FakeSmsProvider provider;

    @Autowired
    private JsonMapper jsonMapper;

    @Test
    void thirdSmsInABurstWaitsForItsSlotAndIsThenSentWithoutANewToken() throws Exception {
        String user = "user-" + UUID.randomUUID().toString().substring(0, 8);
        List<UUID> ids = List.of(insertSms(user), insertSms(user), insertSms(user));
        for (UUID id : ids) {
            publish(user, id);
        }

        await().atMost(TIMEOUT).until(() -> count(ids, "SENT") == 2 && count(ids, "DEFERRED") == 1);
        UUID deferred = ids.stream().filter(id -> "DEFERRED".equals(statusOf(id))).findFirst().orElseThrow();
        Instant slot = jdbc.sql("SELECT deliver_after FROM notifications WHERE id = :id")
                .param("id", deferred).query(Timestamp.class).single().toInstant();
        assertThat(Duration.between(Instant.now(), slot)).isBetween(Duration.ofMinutes(59), Duration.ofMinutes(61));
        assertThat(provider.attempts(deferred.toString())).isZero();

        // What the API's release job does when the slot comes; the bucket is still empty for an hour.
        jdbc.sql("UPDATE notifications SET status = 'QUEUED', deliver_after = NULL WHERE id = :id")
                .param("id", deferred).update();
        publish(user, deferred);

        await().atMost(TIMEOUT).until(() -> "SENT".equals(statusOf(deferred)));
        assertThat(provider.attempts(deferred.toString())).isEqualTo(1);
    }

    private UUID insertSms(String user) {
        UUID id = UUID.randomUUID();
        Timestamp now = Timestamp.from(Instant.now());
        jdbc.sql("""
                        INSERT INTO notifications
                            (id, user_id, channel, recipient, subject, body, status, created_at, updated_at)
                        VALUES (:id, :user, 'SMS', '+6591234567', NULL, 'Code', 'QUEUED', :now, :now)
                        """)
                .param("id", id)
                .param("user", user)
                .param("now", now)
                .update();
        return id;
    }

    private void publish(String user, UUID id) throws Exception {
        kafka.send("notifications.sms", user, jsonMapper.writeValueAsString(
                new NotificationMessage(id, user, "SMS", "+6591234567", null, "Code", Instant.now()))).get();
    }

    private long count(List<UUID> ids, String status) {
        return ids.stream().filter(id -> status.equals(statusOf(id))).count();
    }

    private String statusOf(UUID id) {
        return jdbc.sql("SELECT status FROM notifications WHERE id = :id").param("id", id).query(String.class).single();
    }
}
