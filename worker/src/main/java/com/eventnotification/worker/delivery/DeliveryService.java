package com.eventnotification.worker.delivery;

import com.eventnotification.worker.config.WorkerProperties;
import com.eventnotification.worker.preference.UserPreferences;
import com.eventnotification.worker.preference.UserPreferencesRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

/**
 * Delivers one notification through a channel's sender, at most once per notification.
 *
 * <p>Before sending, the worker claims the notification in {@code delivery_claims}. The claim
 * is atomic, so two workers that receive the same message (after a rebalance, say) cannot both
 * send it. A duplicate is still possible if a worker crashes after sending but before marking
 * the notification SENT; see docs/DESIGN.md.
 */
@Service
public class DeliveryService {

    private static final Logger log = LoggerFactory.getLogger(DeliveryService.class);

    private final NotificationStatusRepository statuses;
    private final DeliveryClaimRepository claims;
    private final UserPreferencesRepository preferences;
    private final Duration claimLease;
    private final Clock clock;
    private final String workerId = UUID.randomUUID().toString();

    public DeliveryService(NotificationStatusRepository statuses, DeliveryClaimRepository claims,
                           UserPreferencesRepository preferences, WorkerProperties properties, Clock clock) {
        this.statuses = statuses;
        this.claims = claims;
        this.preferences = preferences;
        this.claimLease = properties.delivery().claimLease();
        this.clock = clock;
    }

    /**
     * Sends the notification, or skips it if it was already handled.
     *
     * @throws TransientDeliveryException if sending failed in a way a retry may fix, or another
     *                                    worker holds a live claim; the caller retries
     */
    public DeliveryOutcome deliver(NotificationMessage message, ChannelSender sender) {
        UUID id = message.notificationId();
        // Cheap check first, so redelivered messages for finished notifications skip the claim.
        Optional<DeliveryOutcome> skip = skipReason(id);
        if (skip.isPresent()) {
            return skip.get();
        }

        if (!claims.tryClaim(id, workerId, claimLease)) {
            // Never acknowledge a message we did not settle: if the claimant has crashed, a
            // retry after its lease expires takes the notification over.
            throw new TransientDeliveryException("Notification " + id + " is claimed by another worker", null);
        }
        // Check again: the previous claimant may have finished just before its lease expired.
        skip = skipReason(id);
        if (skip.isPresent()) {
            return skip.get();
        }
        // Only after claiming: otherwise one worker could defer a notification another is sending.
        Optional<DeliveryOutcome> held = applyPreferences(message);
        if (held.isPresent()) {
            return held.get();
        }

        try {
            sender.send(message);
        } catch (PermanentDeliveryException e) {
            log.warn("Notification {} failed permanently ({}): {}", id, e.failureCode(), e.getMessage());
            statuses.markFailed(id, e.failureCode(), clock.instant());
            return DeliveryOutcome.FAILED;
        } catch (RuntimeException e) {
            release(id);
            throw e;
        }

        statuses.markSent(id, clock.instant());
        log.info("Sent {} notification {} to user {}", message.channel(), id, message.userId());
        return DeliveryOutcome.SENT;
    }

    /**
     * Suppresses the notification if the user turned its channel off, or defers it until the
     * user's quiet hours end. Preferences are read at delivery time, so a change made after the
     * notification was created still applies.
     */
    private Optional<DeliveryOutcome> applyPreferences(NotificationMessage message) {
        UUID id = message.notificationId();
        UserPreferences preferences = this.preferences.find(message.userId()).orElse(null);
        if (preferences == null) {
            return Optional.empty();
        }
        Instant now = clock.instant();
        if (!preferences.allows(message.channel())) {
            statuses.markSuppressed(id, FailureCodes.CHANNEL_DISABLED, now);
            log.info("Suppressed {} notification {}: user {} turned the channel off", message.channel(), id,
                    message.userId());
            return Optional.of(DeliveryOutcome.SUPPRESSED);
        }
        Optional<Instant> quietUntil = preferences.quietUntil(now);
        if (quietUntil.isPresent()) {
            statuses.markDeferred(id, quietUntil.get(), now);
            // Free the claim, so the message released after quiet hours can be claimed at once.
            release(id);
            log.info("Deferred notification {} until {}: quiet hours for user {}", id, quietUntil.get(),
                    message.userId());
            return Optional.of(DeliveryOutcome.DEFERRED);
        }
        return Optional.empty();
    }

    /** Lets the retry claim straight away instead of waiting for the lease to expire. */
    private void release(UUID id) {
        try {
            claims.release(id, workerId);
        } catch (RuntimeException e) {
            log.warn("Could not release claim on notification {}; it expires after {}", id, claimLease, e);
        }
    }

    private Optional<DeliveryOutcome> skipReason(UUID id) {
        Optional<String> status = statuses.findStatus(id);
        if (status.isEmpty()) {
            log.warn("Skipping unknown notification {}", id);
            return Optional.of(DeliveryOutcome.SKIPPED_UNKNOWN);
        }
        if (!"QUEUED".equals(status.get())) {
            log.info("Skipping notification {} already in status {}", id, status.get());
            return Optional.of(DeliveryOutcome.SKIPPED_ALREADY_PROCESSED);
        }
        return Optional.empty();
    }
}
