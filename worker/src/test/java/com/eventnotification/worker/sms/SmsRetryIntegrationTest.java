package com.eventnotification.worker.sms;

import com.eventnotification.worker.FakeSmsProvider;
import com.eventnotification.worker.TestcontainersConfiguration;
import com.eventnotification.worker.TopicReader;
import com.eventnotification.worker.delivery.FailureCodes;
import com.eventnotification.worker.delivery.MalformedMessageException;
import com.eventnotification.worker.delivery.NotificationMessage;
import com.eventnotification.worker.retry.DeadLetterReplayer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.testcontainers.kafka.KafkaContainer;
import tools.jackson.databind.json.JsonMapper;

import java.math.BigInteger;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Retry topics, the dead-letter topic and replay, against real Kafka and Postgres. */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
@ActiveProfiles("test")
class SmsRetryIntegrationTest {

    private static final String SMS_TOPIC = "notifications.sms";
    private static final String SMS_DLT = "notifications.sms-dlt";
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @Autowired
    private JdbcClient jdbc;

    @Autowired
    private KafkaTemplate<String, String> kafka;

    @Autowired
    private KafkaContainer kafkaContainer;

    @Autowired
    private FakeSmsProvider provider;

    @Autowired
    private DeadLetterReplayer replayer;

    @Autowired
    private JsonMapper jsonMapper;

    @AfterEach
    void healthyProvider() {
        provider.respondAlways(null);
    }

    @Test
    void createsRetryAndDeadLetterTopicsWithTheMainTopicsPartitionCount() throws Exception {
        try (AdminClient admin = AdminClient.create(Map.<String, Object>of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers()))) {
            await().atMost(TIMEOUT).untilAsserted(() -> assertThat(admin.listTopics().names().get())
                    .contains(SMS_TOPIC + "-retry-0", SMS_TOPIC + "-retry-1", SMS_TOPIC + "-retry-2", SMS_DLT));
            Set<String> retryTopics = Set.of(SMS_TOPIC + "-retry-0", SMS_DLT);
            admin.describeTopics(retryTopics).allTopicNames().get().values()
                    .forEach(t -> assertThat(t.partitions()).hasSize(6));
        }
    }

    @Test
    void retriesThroughRetryTopicsUntilTheProviderRecovers() throws Exception {
        UUID id = insertSms("QUEUED");
        provider.script(id.toString(), 503, 503, FakeSmsProvider.TIMEOUT);

        publish(id);

        await().atMost(TIMEOUT).until(() -> "SENT".equals(statusOf(id)));
        assertThat(provider.attempts(id.toString())).isEqualTo(4);
    }

    @Test
    void eachRetriedRecordGetsItsOwnJitteredDelay() throws Exception {
        // Test settings: retry-0 waits 200ms +/- 50ms. Spring alone would give every record exactly 200ms.
        Set<String> ids = new HashSet<>();
        for (int i = 0; i < 20; i++) {
            UUID id = insertSms("QUEUED");
            provider.script(id.toString(), 503);
            ids.add(id.toString());
            publish(id);
        }

        List<ConsumerRecord<String, String>> retried = TopicReader.awaitRecords(
                kafkaContainer.getBootstrapServers(), SMS_TOPIC + "-retry-0",
                r -> r.value() != null && ids.stream().anyMatch(r.value()::contains), ids.size());

        List<Long> delays = retried.stream()
                .map(r -> new BigInteger(r.headers().lastHeader("retry_topic-backoff-timestamp").value()).longValue()
                        - r.timestamp())
                .toList();
        assertThat(delays).allSatisfy(d -> assertThat(d).isBetween(140L, 270L));
        assertThat(Collections.max(delays) - Collections.min(delays)).isGreaterThan(30L);
    }

    @Test
    void rejectedRecipientFailsAtOnceWithoutRetrying() throws Exception {
        UUID id = insertSms("QUEUED");
        provider.script(id.toString(), 400);

        publish(id);

        await().atMost(TIMEOUT).until(() -> "FAILED".equals(statusOf(id)));
        assertThat(failureReasonOf(id)).isEqualTo(FailureCodes.RECIPIENT_REJECTED);
        Thread.sleep(1_500); // longer than the whole test retry schedule
        assertThat(provider.attempts(id.toString())).isEqualTo(1);
    }

    @Test
    void exhaustedRetriesDeadLetterTheMessageAndReplayDeliversIt() throws Exception {
        UUID id = insertSms("QUEUED");
        provider.script(id.toString(), 503, 503, 503, 503);

        publish(id);

        await().atMost(TIMEOUT).until(() -> "FAILED".equals(statusOf(id)));
        assertThat(failureReasonOf(id)).isEqualTo(FailureCodes.RETRIES_EXHAUSTED);
        assertThat(provider.attempts(id.toString())).isEqualTo(4);
        ConsumerRecord<String, String> deadLetter = TopicReader.awaitRecord(kafkaContainer.getBootstrapServers(),
                SMS_DLT, r -> r.value() != null && r.value().contains(id.toString()));
        assertThat(deadLetter.key()).isEqualTo("user-1");

        // The provider has recovered (the script is used up); replay sends the message again.
        DeadLetterReplayer.Result result = replayer.replay(SMS_DLT, 1_000);

        assertThat(result.replayed()).isGreaterThanOrEqualTo(1);
        await().atMost(TIMEOUT).until(() -> "SENT".equals(statusOf(id)));
        assertThat(provider.attempts(id.toString())).isEqualTo(5);
    }

    @Test
    void replayDoesNotResendNotificationsThatAreAlreadySent() throws Exception {
        UUID id = insertSms("QUEUED");
        provider.script(id.toString(), 503, 503, 503, 503);
        publish(id);
        await().atMost(TIMEOUT).until(() -> "FAILED".equals(statusOf(id)));
        replayer.replay(SMS_DLT, 1_000);
        await().atMost(TIMEOUT).until(() -> "SENT".equals(statusOf(id)));

        // The committed offset means a second run finds nothing new for this notification.
        replayer.replay(SMS_DLT, 1_000);
        Thread.sleep(1_000);

        assertThat(provider.attempts(id.toString())).isEqualTo(5);
    }

    @Test
    void malformedMessageGoesStraightToTheDeadLetterTopic() {
        String poison = "{not json " + UUID.randomUUID();

        kafka.send(SMS_TOPIC, "user-1", poison);

        ConsumerRecord<String, String> deadLetter = TopicReader.awaitRecord(kafkaContainer.getBootstrapServers(),
                SMS_DLT, r -> poison.equals(r.value()));
        assertThat(new String(deadLetter.headers().lastHeader("kafka_exception-cause-fqcn").value()))
                .isEqualTo(MalformedMessageException.class.getName());
    }

    private UUID insertSms(String status) {
        UUID id = UUID.randomUUID();
        Timestamp now = Timestamp.from(Instant.now());
        jdbc.sql("""
                        INSERT INTO notifications
                            (id, user_id, channel, recipient, subject, body, status, created_at, updated_at)
                        VALUES (:id, 'user-1', 'SMS', '+6591234567', NULL, 'Your code', :status, :now, :now)
                        """)
                .param("id", id)
                .param("status", status)
                .param("now", now)
                .update();
        return id;
    }

    private void publish(UUID id) throws Exception {
        String value = jsonMapper.writeValueAsString(new NotificationMessage(id, "user-1", "SMS", "+6591234567",
                null, "Your code", Instant.now()));
        kafka.send(SMS_TOPIC, "user-1", value).get();
    }

    private String statusOf(UUID id) {
        return jdbc.sql("SELECT status FROM notifications WHERE id = :id").param("id", id).query(String.class).single();
    }

    private String failureReasonOf(UUID id) {
        return jdbc.sql("SELECT failure_reason FROM notifications WHERE id = :id")
                .param("id", id).query(String.class).single();
    }
}
