package com.eventnotification.worker.email;

import com.eventnotification.worker.TestWorkerProperties;
import com.eventnotification.worker.delivery.FailureCodes;
import com.eventnotification.worker.delivery.NotificationMessage;
import com.eventnotification.worker.delivery.PermanentDeliveryException;
import com.eventnotification.worker.delivery.TransientDeliveryException;
import jakarta.mail.MessagingException;
import jakarta.mail.SendFailedException;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.mail.MailSendException;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class EmailSenderTest {

    private final JavaMailSender mailSender = mock(JavaMailSender.class);
    private final EmailSender sender = new EmailSender(mailSender, TestWorkerProperties.create());

    private final NotificationMessage message = new NotificationMessage(UUID.randomUUID(), "user-1", "EMAIL",
            "bob@example.com", "Hello", "Body text", Instant.now());

    @Test
    void sendsMailWithConfiguredSender() {
        sender.send(message);

        ArgumentCaptor<SimpleMailMessage> mail = ArgumentCaptor.forClass(SimpleMailMessage.class);
        verify(mailSender).send(mail.capture());
        assertThat(mail.getValue().getFrom()).isEqualTo("no-reply@example.com");
        assertThat(mail.getValue().getTo()).containsExactly("bob@example.com");
        assertThat(mail.getValue().getSubject()).isEqualTo("Hello");
        assertThat(mail.getValue().getText()).isEqualTo("Body text");
    }

    @Test
    void rejectedRecipientIsPermanentWithPublicCode() {
        doThrow(new MailSendException("Failed messages",
                new SendFailedException("550 5.1.1 mailbox unavailable on smtp.internal:25")))
                .when(mailSender).send(any(SimpleMailMessage.class));

        assertThatThrownBy(() -> sender.send(message))
                .isInstanceOfSatisfying(PermanentDeliveryException.class,
                        e -> assertThat(e.failureCode()).isEqualTo(FailureCodes.RECIPIENT_REJECTED));
    }

    @Test
    void unreachableMailServerIsTransient() {
        doThrow(new MailSendException("Mail server connection failed",
                new MessagingException("Couldn't connect to host, port: smtp.internal, 25")))
                .when(mailSender).send(any(SimpleMailMessage.class));

        assertThatThrownBy(() -> sender.send(message)).isInstanceOf(TransientDeliveryException.class);
    }
}
