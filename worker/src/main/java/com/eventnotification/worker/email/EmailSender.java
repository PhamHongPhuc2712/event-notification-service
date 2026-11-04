package com.eventnotification.worker.email;

import com.eventnotification.worker.config.WorkerProperties;
import com.eventnotification.worker.delivery.ChannelSender;
import com.eventnotification.worker.delivery.FailureCodes;
import com.eventnotification.worker.delivery.NotificationMessage;
import com.eventnotification.worker.delivery.PermanentDeliveryException;
import com.eventnotification.worker.delivery.TransientDeliveryException;
import jakarta.mail.SendFailedException;
import org.springframework.mail.MailException;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Component;

@Component
public class EmailSender implements ChannelSender {

    private final JavaMailSender mailSender;
    private final String from;

    public EmailSender(JavaMailSender mailSender, WorkerProperties properties) {
        this.mailSender = mailSender;
        this.from = properties.mail().from();
    }

    @Override
    public void send(NotificationMessage message) {
        try {
            mailSender.send(toMail(message));
        } catch (MailException e) {
            if (isPermanent(e)) {
                throw new PermanentDeliveryException(FailureCodes.RECIPIENT_REJECTED,
                        "Mail server rejected notification " + message.notificationId() + ": " + e.getMessage(), e);
            }
            throw new TransientDeliveryException("Mail server unavailable for notification "
                    + message.notificationId(), e);
        }
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
