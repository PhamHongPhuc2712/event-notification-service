package com.eventnotification.worker.retry;

import com.eventnotification.worker.TestWorkerProperties;
import com.eventnotification.worker.delivery.FailureCodes;
import com.eventnotification.worker.delivery.MessageParser;
import com.eventnotification.worker.delivery.NotificationStatusRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class DeadLetterHandlerTest {

    private static final Instant NOW = Instant.parse("2026-11-09T12:00:00Z");
    private static final String DLT = "notifications.sms-dlt";

    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final NotificationStatusRepository statuses = mock(NotificationStatusRepository.class);
    private final DeadLetterHandler handler = new DeadLetterHandler(new MessageParser(JsonMapper.builder().build()),
            statuses, TestWorkerProperties.create(), registry, Clock.fixed(NOW, ZoneOffset.UTC));

    @Test
    void marksTheNotificationFailedAndCountsTheDeadLetterByTopic() {
        UUID id = UUID.randomUUID();

        handler.handle(new ConsumerRecord<>(DLT, 0, 0, "user-1", "{\"notificationId\":\"" + id + "\"}"));

        verify(statuses).markFailed(id, FailureCodes.RETRIES_EXHAUSTED, NOW);
        assertThat(deadLetters()).isEqualTo(1.0);
    }

    @Test
    void startsEveryDeadLetterTopicAtZeroSoTheFirstOneShowsInRates() {
        // Prometheus increase() cannot see a series that is born at 1.
        assertThat(registry.find(DeadLetterHandler.DEAD_LETTERS).counters())
                .extracting(c -> c.getId().getTag("topic"))
                .containsExactlyInAnyOrder("notifications.email-dlt", "notifications.email.high-dlt",
                        "notifications.sms-dlt", "notifications.sms.high-dlt",
                        "notifications.webhook-dlt", "notifications.webhook.high-dlt");
        assertThat(deadLetters()).isZero();
    }

    @Test
    void countsMalformedDeadLettersToo() {
        handler.handle(new ConsumerRecord<>(DLT, 0, 1, "user-1", "not json"));

        verify(statuses, org.mockito.Mockito.never()).markFailed(any(), eq(FailureCodes.RETRIES_EXHAUSTED), any());
        assertThat(deadLetters()).isEqualTo(1.0);
    }

    private double deadLetters() {
        return registry.get(DeadLetterHandler.DEAD_LETTERS).tag("topic", DLT).counter().count();
    }
}
