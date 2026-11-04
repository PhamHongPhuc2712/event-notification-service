package com.eventnotification.worker.delivery;

import com.eventnotification.worker.config.WorkerProperties;
import jakarta.mail.SendFailedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.mail.MailException;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;

/**
 * Delivers one email notification.
 *
 * <p>Before sending, the worker claims the notification in {@code delivery_claims}. The claim
 * is atomic, so two workers that receive the same message (after a rebalance, say) cannot both
 * send it. A duplicate is still possible if a worker crashes after sending but before marking
 * the notification SENT; see docs/DESIGN.md.
 */
@Service
public class EmailDeliveryService {

    /** Public failure code. Raw SMTP errors stay in the logs, because they reveal internal hosts. */
    static final String RECIPIENT_REJECTED = "RECIPIENT_REJECTED";

    private static final Logger log = LoggerFactory.getLogger(EmailDeliveryService.class);

    private final JavaMailSender mailSender;
    private final NotificationStatusRepository statuses;
    private final DeliveryClaimRepository claims;
    private final String from;
    private final Duration claimLease;
    private final Clock clock;
    private final String workerId = UUID.randomUUID().toString();

    public EmailDeliveryService(JavaMailSender mailSender, NotificationStatusRepository statuses,
                                DeliveryClaimRepository claims, WorkerProperties properties, Clock clock) {
        this.mailSender = mailSender;
        this.statuses = statuses;
        this.claims = claims;
        this.from = properties.mail().from();
        this.claimLease = properties.delivery().claimLease();
        this.clock = clock;
    }

    /**
     * Sends the email, or skips it if it was already handled.
     *
     * @throws TransientDeliveryException if the mail server could not be reached, or another
     *                                    worker holds a live claim; the caller retries
     */
    public DeliveryOutcome deliver(NotificationMessage message) {
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

        try {
            mailSender.send(toMail(message));
        } catch (MailException e) {
            if (!isPermanent(e)) {
                claims.release(id, workerId);
                throw new TransientDeliveryException("Mail server unavailable for notification " + id, e);
            }
            log.warn("Email for notification {} rejected permanently: {}", message.notificationId(), e.getMessage());
            statuses.markFailed(message.notificationId(), RECIPIENT_REJECTED, clock.instant());
            return DeliveryOutcome.FAILED;
        }

        statuses.markSent(message.notificationId(), clock.instant());
        log.info("Sent notification {} to user {}", message.notificationId(), message.userId());
        return DeliveryOutcome.SENT;
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

    /**
     * The server accepted the connection but refused the message or its recipients, so
     * retrying cannot help. Connection, timeout and authentication errors are transient.
     */
    private static boolean isPermanent(MailException e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof SendFailedException) {
                return true;
            }
        }
        return false;
    }

    private SimpleMailMessage toMail(NotificationMessage message) {
        SimpleMailMessage mail = new SimpleMailMessage();
        mail.setFrom(from);
        mail.setTo(message.recipient());
        mail.setSubject(message.subject());
        mail.setText(message.body());
        return mail;
    }
}
