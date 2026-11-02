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
import java.util.Optional;

/**
 * Delivers one email notification.
 *
 * <p>The status check below stops redelivered messages from being sent again in the common
 * case. It is not race-proof: two workers can both see QUEUED. Weeks 3-4 replace it with an
 * atomic claim backed by a unique constraint.
 */
@Service
public class EmailDeliveryService {

    /** Public failure code. Raw SMTP errors stay in the logs, because they reveal internal hosts. */
    static final String RECIPIENT_REJECTED = "RECIPIENT_REJECTED";

    private static final Logger log = LoggerFactory.getLogger(EmailDeliveryService.class);

    private final JavaMailSender mailSender;
    private final NotificationStatusRepository statuses;
    private final String from;
    private final Clock clock;

    public EmailDeliveryService(JavaMailSender mailSender, NotificationStatusRepository statuses,
                                WorkerProperties properties, Clock clock) {
        this.mailSender = mailSender;
        this.statuses = statuses;
        this.from = properties.mail().from();
        this.clock = clock;
    }

    /**
     * Sends the email, or skips it if it was already handled.
     *
     * @throws TransientDeliveryException if the mail server could not be reached; the caller retries
     */
    public DeliveryOutcome deliver(NotificationMessage message) {
        Optional<String> status = statuses.findStatus(message.notificationId());
        if (status.isEmpty()) {
            log.warn("Skipping unknown notification {}", message.notificationId());
            return DeliveryOutcome.SKIPPED_UNKNOWN;
        }
        if (!"QUEUED".equals(status.get())) {
            log.info("Skipping notification {} already in status {}", message.notificationId(), status.get());
            return DeliveryOutcome.SKIPPED_ALREADY_PROCESSED;
        }

        try {
            mailSender.send(toMail(message));
        } catch (MailException e) {
            if (!isPermanent(e)) {
                throw new TransientDeliveryException("Mail server unavailable for notification "
                        + message.notificationId(), e);
            }
            log.warn("Email for notification {} rejected permanently: {}", message.notificationId(), e.getMessage());
            statuses.markFailed(message.notificationId(), RECIPIENT_REJECTED, clock.instant());
            return DeliveryOutcome.FAILED;
        }

        statuses.markSent(message.notificationId(), clock.instant());
        log.info("Sent notification {} to user {}", message.notificationId(), message.userId());
        return DeliveryOutcome.SENT;
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
