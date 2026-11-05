package com.eventnotification.worker.sms;

import com.eventnotification.worker.FakeSmsProvider;
import com.eventnotification.worker.TestcontainersConfiguration;
import com.eventnotification.worker.delivery.NotificationMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;
import tools.jackson.databind.json.JsonMapper;

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

/** HIGH-priority notifications overtake a backlog and ignore quiet hours. */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
@ActiveProfiles("test")
class SmsPriorityIntegrationTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @Autowired
    private JdbcClient jdbc;

    @Autowired
    private KafkaTemplate<String, String> kafka;

    @Autowired
    private FakeSmsProvider provider;

    @Autowired
    private JsonMapper jsonMapper;

    @AfterEach
    void fastProvider() {
        provider.setLatency(Duration.ZERO);
    }

    @Test
    void highPrioritySmsOvertakesTheNormalBacklog() throws Exception {
        // 3 normal consumer threads at 300ms a send: 60 notifications take about 6 seconds.
        provider.setLatency(Duration.ofMillis(300));
        List<UUID> backlog = new ArrayList<>();
        for (int i = 0; i < 60; i++) {
            String user = uniqueUser();
            UUID id = insertSms(user, "NORMAL");
            backlog.add(id);
            publish("notifications.sms", user, id, "NORMAL");
        }
        await().atMost(TIMEOUT).until(() -> countSent(backlog) > 0);

        String user = uniqueUser();
        UUID reset = insertSms(user, "HIGH");
        publish("notifications.sms.high", user, reset, NotificationMessage.HIGH);

        await().atMost(TIMEOUT).until(() -> "SENT".equals(statusOf(reset)));
        long backlogStillWaiting = backlog.size() - countSent(backlog);
        assertThat(backlogStillWaiting).as("normal notifications still queued when the reset was sent")
                .isGreaterThan(20);
    }

    @Test
    void highPrioritySmsIsSentDuringQuietHours() throws Exception {
        String user = uniqueUser();
        LocalTime now = LocalTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.MINUTES);
        jdbc.sql("""
                        INSERT INTO user_preferences (user_id, allowed_channels, quiet_start, quiet_end, time_zone,
                                                      updated_at)
                        VALUES (:user, '{SMS}', :start, :end, 'UTC', now())
                        """)
                .param("user", user)
                .param("start", Time.valueOf(now.minusHours(1)))
                .param("end", Time.valueOf(now.plusHours(1)))
                .update();
        UUID marketing = insertSms(user, "NORMAL");
        UUID reset = insertSms(user, "HIGH");

        publish("notifications.sms", user, marketing, "NORMAL");
        publish("notifications.sms.high", user, reset, NotificationMessage.HIGH);

        await().atMost(TIMEOUT).until(() -> "SENT".equals(statusOf(reset)) && "DEFERRED".equals(statusOf(marketing)));
    }

    private UUID insertSms(String user, String priority) {
        UUID id = UUID.randomUUID();
        Timestamp now = Timestamp.from(Instant.now());
        jdbc.sql("""
                        INSERT INTO notifications (id, user_id, channel, recipient, subject, body, priority, status,
                                                   created_at, updated_at)
                        VALUES (:id, :user, 'SMS', '+6591234567', NULL, 'Text', :priority, 'QUEUED', :now, :now)
                        """)
                .param("id", id)
                .param("user", user)
                .param("priority", priority)
                .param("now", now)
                .update();
        return id;
    }

    private void publish(String topic, String user, UUID id, String priority) throws Exception {
        kafka.send(topic, user, jsonMapper.writeValueAsString(new NotificationMessage(id, user, "SMS", "+6591234567",
                null, "Text", priority, Instant.now()))).get();
    }

    private long countSent(List<UUID> ids) {
        return jdbc.sql("SELECT count(*) FROM notifications WHERE status = 'SENT' AND id IN (:ids)")
                .param("ids", ids)
                .query(Long.class)
                .single();
    }

    private String statusOf(UUID id) {
        return jdbc.sql("SELECT status FROM notifications WHERE id = :id").param("id", id).query(String.class).single();
    }

    private static String uniqueUser() {
        return "user-" + UUID.randomUUID().toString().substring(0, 8);
    }
}
