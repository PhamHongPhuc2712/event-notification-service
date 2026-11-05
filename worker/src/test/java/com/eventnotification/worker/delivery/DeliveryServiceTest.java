package com.eventnotification.worker.delivery;

import com.eventnotification.worker.TestWorkerProperties;
import com.eventnotification.worker.preference.QuietHours;
import com.eventnotification.worker.preference.UserPreferences;
import com.eventnotification.worker.preference.UserPreferencesRepository;
import com.eventnotification.worker.ratelimit.RateLimiter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.Set;
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
    private final UserPreferencesRepository preferences = mock(UserPreferencesRepository.class);
    private final RateLimiter rateLimiter = mock(RateLimiter.class);
    private final DeliveryService service = new DeliveryService(statuses, claims, preferences, rateLimiter,
            TestWorkerProperties.create(), Clock.fixed(NOW, ZoneOffset.UTC));

    private final NotificationMessage message = new NotificationMessage(UUID.randomUUID(), "user-1", "EMAIL",
            "bob@example.com", "Hello", "Body text", NOW);

    @BeforeEach
    void notRateLimited() {
        when(rateLimiter.acquire(anyString(), anyString())).thenReturn(RateLimiter.Decision.NOW);
    }

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

    @Test
    void suppressesNotificationOnAChannelTheUserTurnedOff() {
        givenQueuedAndClaimable();
        givenPreferences(new UserPreferences(Set.of("SMS"), Optional.empty()));

        assertThat(service.deliver(message, sender)).isEqualTo(DeliveryOutcome.SUPPRESSED);

        verify(statuses).markSuppressed(message.notificationId(), FailureCodes.CHANNEL_DISABLED, NOW);
        verify(sender, never()).send(any());
        verify(statuses, never()).markSent(any(), any());
    }

    @Test
    void defersNotificationUntilQuietHoursEnd() {
        givenQueuedAndClaimable();
        // NOW is 12:00 UTC, inside 11:00-13:00.
        givenPreferences(new UserPreferences(Set.of("EMAIL"),
                Optional.of(new QuietHours(LocalTime.of(11, 0), LocalTime.of(13, 0), ZoneOffset.UTC))));

        assertThat(service.deliver(message, sender)).isEqualTo(DeliveryOutcome.DEFERRED);

        verify(statuses).markDeferred(message.notificationId(), Instant.parse("2026-10-30T13:00:00Z"), NOW);
        verify(claims).release(eq(message.notificationId()), anyString());
        verify(sender, never()).send(any());
    }

    @Test
    void sendsOnAnAllowedChannelOutsideQuietHours() {
        givenQueuedAndClaimable();
        givenPreferences(new UserPreferences(Set.of("EMAIL"),
                Optional.of(new QuietHours(LocalTime.of(22, 0), LocalTime.of(7, 0), ZoneOffset.UTC))));

        assertThat(service.deliver(message, sender)).isEqualTo(DeliveryOutcome.SENT);

        verify(sender).send(message);
    }

    @Test
    void doesNotApplyPreferencesBeforeWinningTheClaim() {
        when(statuses.findStatus(message.notificationId())).thenReturn(Optional.of("QUEUED"));
        when(claims.tryClaim(eq(message.notificationId()), anyString(), eq(LEASE))).thenReturn(false);
        givenPreferences(new UserPreferences(Set.of(), Optional.empty()));

        assertThatThrownBy(() -> service.deliver(message, sender)).isInstanceOf(TransientDeliveryException.class);

        verify(statuses, never()).markSuppressed(any(), anyString(), any());
    }

    @Test
    void defersARateLimitedNotificationToItsReservedSlot() {
        givenQueuedAndClaimable();
        when(rateLimiter.acquire("user-1", "EMAIL"))
                .thenReturn(new RateLimiter.Decision(RateLimiter.Outcome.LATER, Duration.ofSeconds(12)));

        assertThat(service.deliver(message, sender)).isEqualTo(DeliveryOutcome.DEFERRED);

        verify(statuses).markDeferredForRateLimit(message.notificationId(), NOW.plusSeconds(12), NOW);
        verify(claims).release(eq(message.notificationId()), anyString());
        verify(sender, never()).send(any());
    }

    @Test
    void suppressesANotificationWhoseSlotIsTooFarAway() {
        givenQueuedAndClaimable();
        when(rateLimiter.acquire("user-1", "EMAIL"))
                .thenReturn(new RateLimiter.Decision(RateLimiter.Outcome.REJECTED, Duration.ofHours(2)));

        assertThat(service.deliver(message, sender)).isEqualTo(DeliveryOutcome.SUPPRESSED);

        verify(statuses).markSuppressed(message.notificationId(), FailureCodes.RATE_LIMITED, NOW);
        verify(sender, never()).send(any());
    }

    @Test
    void notificationWithAReservedSlotDoesNotTakeAnotherToken() {
        givenQueuedAndClaimable();
        when(statuses.hasRateLimitSlot(message.notificationId())).thenReturn(true);

        assertThat(service.deliver(message, sender)).isEqualTo(DeliveryOutcome.SENT);

        verify(rateLimiter, never()).acquire(anyString(), anyString());
    }

    @Test
    void failedSendKeepsItsTokenForTheRetry() {
        givenQueuedAndClaimable();
        doThrow(new TransientDeliveryException("provider down", null)).when(sender).send(message);

        assertThatThrownBy(() -> service.deliver(message, sender)).isInstanceOf(TransientDeliveryException.class);

        verify(statuses).markRateLimitSlotTaken(message.notificationId());
    }

    @Test
    void quietHoursAreCheckedBeforeTakingAToken() {
        givenQueuedAndClaimable();
        givenPreferences(new UserPreferences(Set.of("EMAIL"),
                Optional.of(new QuietHours(LocalTime.of(11, 0), LocalTime.of(13, 0), ZoneOffset.UTC))));

        service.deliver(message, sender);

        verify(rateLimiter, never()).acquire(anyString(), anyString());
    }

    private void givenPreferences(UserPreferences userPreferences) {
        when(preferences.find(message.userId())).thenReturn(Optional.of(userPreferences));
    }

    private void givenQueuedAndClaimable() {
        when(statuses.findStatus(message.notificationId())).thenReturn(Optional.of("QUEUED"));
        when(claims.tryClaim(eq(message.notificationId()), anyString(), eq(LEASE))).thenReturn(true);
    }
}
