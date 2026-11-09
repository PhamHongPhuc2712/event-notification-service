package com.eventnotification.worker.delivery;

import com.eventnotification.worker.TestWorkerProperties;
import com.eventnotification.worker.preference.UserPreferencesRepository;
import com.eventnotification.worker.ratelimit.RateLimiter;
import io.micrometer.core.instrument.MockClock;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** The metrics {@link DeliveryService} records for each delivery attempt. */
class DeliveryMetricsTest {

    private static final Instant NOW = Instant.parse("2026-11-09T12:00:00Z");

    private final MockClock meterClock = new MockClock();
    private final SimpleMeterRegistry registry = new SimpleMeterRegistry(SimpleConfig.DEFAULT, meterClock);
    private final ChannelSender sender = mock(ChannelSender.class);
    private final NotificationStatusRepository statuses = mock(NotificationStatusRepository.class);
    private final DeliveryClaimRepository claims = mock(DeliveryClaimRepository.class);
    private final RateLimiter rateLimiter = mock(RateLimiter.class);
    private final DeliveryService service = new DeliveryService(statuses, claims,
            mock(UserPreferencesRepository.class), rateLimiter, new DeliveryMetrics(registry),
            TestWorkerProperties.create(), Clock.fixed(NOW, ZoneOffset.UTC));

    /** Created 150 ms before the worker's clock reads NOW. */
    private final NotificationMessage message = new NotificationMessage(UUID.randomUUID(), "user-1", "SMS",
            "+6591234567", null, "Your code is 123456", "HIGH", NOW.minusMillis(150));

    @BeforeEach
    void queuedAndClaimable() {
        when(rateLimiter.acquire(anyString(), anyString(), anyBoolean())).thenReturn(RateLimiter.Decision.NOW);
        when(statuses.findStatus(message.notificationId())).thenReturn(Optional.of("QUEUED"));
        when(claims.tryClaim(eq(message.notificationId()), anyString(), any())).thenReturn(true);
    }

    @Test
    void countsTheOutcomeByChannelAndPriority() {
        service.deliver(message, sender);

        assertThat(deliveries("sent")).isEqualTo(1.0);
    }

    @Test
    void recordsEndToEndLatencyFromCreationUntilSent() {
        service.deliver(message, sender);

        Timer latency = registry.get(DeliveryMetrics.LATENCY).tags("channel", "SMS", "priority", "HIGH").timer();
        assertThat(latency.count()).isEqualTo(1);
        assertThat(latency.totalTime(TimeUnit.MILLISECONDS)).isEqualTo(150.0);
    }

    @Test
    void recordsHowLongTheProviderTookBySendResult() {
        doAnswer(invocation -> {
            meterClock.add(Duration.ofMillis(40));
            return null;
        }).when(sender).send(message);

        service.deliver(message, sender);

        assertThat(sendTimer("success").totalTime(TimeUnit.MILLISECONDS)).isEqualTo(40.0);
    }

    @Test
    void countsATransientFailureAsARetry() {
        doAnswer(invocation -> {
            throw new TransientDeliveryException("provider down", null);
        }).when(sender).send(message);

        assertThatThrownBy(() -> service.deliver(message, sender)).isInstanceOf(TransientDeliveryException.class);

        assertThat(deliveries("retry")).isEqualTo(1.0);
        assertThat(sendTimer("transient_failure").count()).isEqualTo(1);
        assertThat(registry.find(DeliveryMetrics.LATENCY).timer()).isNull();
    }

    @Test
    void countsAPermanentFailure() {
        doAnswer(invocation -> {
            throw new PermanentDeliveryException(FailureCodes.RECIPIENT_REJECTED, "unreachable", null);
        }).when(sender).send(message);

        service.deliver(message, sender);

        assertThat(deliveries("failed")).isEqualTo(1.0);
        assertThat(sendTimer("permanent_failure").count()).isEqualTo(1);
    }

    @Test
    void countsAnUnexpectedErrorSeparatelyFromProviderRetries() {
        when(statuses.findStatus(message.notificationId())).thenThrow(new IllegalStateException("database down"));

        assertThatThrownBy(() -> service.deliver(message, sender)).isInstanceOf(IllegalStateException.class);

        assertThat(deliveries("error")).isEqualTo(1.0);
    }

    @Test
    void countsSkippedRedeliveries() {
        when(statuses.findStatus(message.notificationId())).thenReturn(Optional.of("SENT"));

        service.deliver(message, sender);

        assertThat(deliveries("skipped_already_processed")).isEqualTo(1.0);
    }

    @Test
    void treatsMessagesFromBeforePrioritiesAsNormal() {
        NotificationMessage legacy = new NotificationMessage(message.notificationId(), "user-1", "SMS",
                "+6591234567", null, "Hi", null, null);

        service.deliver(legacy, sender);

        assertThat(registry.get(DeliveryMetrics.DELIVERIES)
                .tags("channel", "SMS", "priority", "NORMAL", "outcome", "sent").counter().count())
                .isEqualTo(1.0);
        // No creation time, so no latency to record.
        assertThat(registry.find(DeliveryMetrics.LATENCY).timer()).isNull();
    }

    private double deliveries(String outcome) {
        return registry.get(DeliveryMetrics.DELIVERIES)
                .tags("channel", "SMS", "priority", "HIGH", "outcome", outcome).counter().count();
    }

    private Timer sendTimer(String result) {
        return registry.get(DeliveryMetrics.SEND).tags("channel", "SMS", "result", result).timer();
    }
}
