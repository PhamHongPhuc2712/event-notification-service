package com.eventnotification.api.outbox;

import com.eventnotification.api.config.NotificationProperties;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OutboxPublisherTest {

    private static final Instant NOW = Instant.parse("2026-10-30T12:00:00Z");

    private final OutboxRepository outbox = mock(OutboxRepository.class);
    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
    private final PlatformTransactionManager txManager = mock(PlatformTransactionManager.class);

    private OutboxPublisher publisher;

    @BeforeEach
    void setUp() {
        NotificationProperties properties = new NotificationProperties(
                new NotificationProperties.Topics("notifications.email", "notifications.sms", 6),
                new NotificationProperties.Outbox(Duration.ofMillis(200), 100, Duration.ofSeconds(1)),
                new NotificationProperties.Deferred(Duration.ofSeconds(1), 100));
        publisher = new OutboxPublisher(outbox, kafka, new TransactionTemplate(txManager), properties,
                Clock.fixed(NOW, ZoneOffset.UTC));
    }

    @Test
    void publishesBatchAndMarksRowsPublished() {
        when(outbox.lockUnpublished(100)).thenReturn(List.of(event(1), event(2)));
        when(kafka.send(anyString(), anyString(), anyString())).thenReturn(succeeded());

        int published = publisher.publishBatch();

        assertThat(published).isEqualTo(2);
        verify(kafka).send("notifications.email", "user-1", "{\"n\":1}");
        verify(kafka).send("notifications.email", "user-2", "{\"n\":2}");
        verify(outbox).markPublished(List.of(1L, 2L), NOW);
    }

    @Test
    void leavesRowsUnpublishedAndRollsBackWhenAnySendFails() {
        when(outbox.lockUnpublished(100)).thenReturn(List.of(event(1), event(2)));
        when(kafka.send("notifications.email", "user-1", "{\"n\":1}")).thenReturn(succeeded());
        when(kafka.send("notifications.email", "user-2", "{\"n\":2}"))
                .thenReturn(CompletableFuture.failedFuture(new TimeoutException("broker unavailable")));

        assertThatThrownBy(publisher::publishBatch)
                .isInstanceOf(OutboxPublishException.class)
                .hasMessageContaining("starting at id 1");

        verify(outbox, never()).markPublished(any(), any());
        verify(txManager).rollback(any());
    }

    @Test
    void emptyOutboxSendsNothing() {
        when(outbox.lockUnpublished(anyInt())).thenReturn(List.of());

        assertThat(publisher.publishBatch()).isZero();

        verify(kafka, never()).send(anyString(), anyString(), anyString());
        verify(outbox, never()).markPublished(any(), any());
    }

    private static OutboxEvent event(long id) {
        return new OutboxEvent(id, UUID.randomUUID(), "notifications.email", "user-" + id, "{\"n\":" + id + "}");
    }

    private static CompletableFuture<SendResult<String, String>> succeeded() {
        return CompletableFuture.completedFuture(null);
    }
}
