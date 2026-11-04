package com.eventnotification.worker.delivery;

import com.eventnotification.worker.TestWorkerProperties;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DeliveryServiceTest {

    private static final Instant NOW = Instant.parse("2026-10-30T12:00:00Z");
    private static final Duration LEASE = TestWorkerProperties.CLAIM_LEASE;

    private final ChannelSender sender = mock(ChannelSender.class);
    private final NotificationStatusRepository statuses = mock(NotificationStatusRepository.class);
    private final DeliveryClaimRepository claims = mock(DeliveryClaimRepository.class);
    private final DeliveryService service = new DeliveryService(statuses, claims, TestWorkerProperties.create(),
            Clock.fixed(NOW, ZoneOffset.UTC));

    private final NotificationMessage message = new NotificationMessage(UUID.randomUUID(), "user-1", "EMAIL",
            "bob@example.com", "Hello", "Body text", NOW);

    @Test
    void sendsQueuedNotificationAndMarksItSent() {
        givenQueuedAndClaimable();

        assertThat(service.deliver(message, sender)).isEqualTo(DeliveryOutcome.SENT);

        verify(sender).send(message);
        verify(statuses).markSent(message.notificationId(), NOW);
    }

    @Test
    void skipsNotificationThatIsNoLongerQueued() {
        when(statuses.findStatus(message.notificationId())).thenReturn(Optional.of("SENT"));

        assertThat(service.deliver(message, sender)).isEqualTo(DeliveryOutcome.SKIPPED_ALREADY_PROCESSED);

        verify(sender, never()).send(any());
        verify(claims, never()).tryClaim(any(), anyString(), any());
    }

    @Test
    void skipsUnknownNotification() {
        when(statuses.findStatus(message.notificationId())).thenReturn(Optional.empty());

        assertThat(service.deliver(message, sender)).isEqualTo(DeliveryOutcome.SKIPPED_UNKNOWN);

        verify(sender, never()).send(any());
    }

    @Test
    void marksNotificationFailedWithTheSendersCodeOnPermanentFailure() {
        givenQueuedAndClaimable();
        doThrow(new PermanentDeliveryException(FailureCodes.RECIPIENT_REJECTED, "550 on smtp.internal:25", null))
                .when(sender).send(message);

        assertThat(service.deliver(message, sender)).isEqualTo(DeliveryOutcome.FAILED);

        verify(statuses).markFailed(message.notificationId(), FailureCodes.RECIPIENT_REJECTED, NOW);
        verify(statuses, never()).markSent(any(), any());
    }

    @Test
    void releasesClaimAndRethrowsOnTransientFailure() {
        givenQueuedAndClaimable();
        TransientDeliveryException failure = new TransientDeliveryException("provider down", null);
        doThrow(failure).when(sender).send(message);

        assertThatThrownBy(() -> service.deliver(message, sender)).isSameAs(failure);

        verify(claims).release(eq(message.notificationId()), anyString());
        verify(statuses, never()).markFailed(any(), anyString(), any());
        verify(statuses, never()).markSent(any(), any());
    }

    @Test
    void stillRethrowsTheSendFailureWhenReleasingTheClaimFails() {
        givenQueuedAndClaimable();
        TransientDeliveryException failure = new TransientDeliveryException("provider down", null);
        doThrow(failure).when(sender).send(message);
        doThrow(new IllegalStateException("database down")).when(claims).release(any(), anyString());

        assertThatThrownBy(() -> service.deliver(message, sender)).isSameAs(failure);
    }

    @Test
    void throwsTransientErrorWithoutSendingWhenAnotherWorkerHoldsTheClaim() {
        when(statuses.findStatus(message.notificationId())).thenReturn(Optional.of("QUEUED"));
        when(claims.tryClaim(eq(message.notificationId()), anyString(), eq(LEASE))).thenReturn(false);

        assertThatThrownBy(() -> service.deliver(message, sender)).isInstanceOf(TransientDeliveryException.class);

        verify(sender, never()).send(any());
        verify(claims, never()).release(any(), anyString());
    }

    @Test
    void skipsWhenPreviousClaimantFinishedJustBeforeClaimWasTakenOver() {
        when(statuses.findStatus(message.notificationId()))
                .thenReturn(Optional.of("QUEUED"))
                .thenReturn(Optional.of("SENT"));
        when(claims.tryClaim(eq(message.notificationId()), anyString(), eq(LEASE))).thenReturn(true);

        assertThat(service.deliver(message, sender)).isEqualTo(DeliveryOutcome.SKIPPED_ALREADY_PROCESSED);

        verify(sender, never()).send(any());
    }

    private void givenQueuedAndClaimable() {
        when(statuses.findStatus(message.notificationId())).thenReturn(Optional.of("QUEUED"));
        when(claims.tryClaim(eq(message.notificationId()), anyString(), eq(LEASE))).thenReturn(true);
    }
}
