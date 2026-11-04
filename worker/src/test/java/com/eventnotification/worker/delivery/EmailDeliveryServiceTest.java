package com.eventnotification.worker.delivery;

import com.eventnotification.worker.config.WorkerProperties;
import jakarta.mail.MessagingException;
import jakarta.mail.SendFailedException;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.mail.MailSendException;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;

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

class EmailDeliveryServiceTest {

    private static final Instant NOW = Instant.parse("2026-10-30T12:00:00Z");
    private static final Duration LEASE = Duration.ofSeconds(60);

    private final JavaMailSender mailSender = mock(JavaMailSender.class);
    private final NotificationStatusRepository statuses = mock(NotificationStatusRepository.class);
    private final DeliveryClaimRepository claims = mock(DeliveryClaimRepository.class);
    private final EmailDeliveryService service = new EmailDeliveryService(mailSender, statuses, claims,
            new WorkerProperties(new WorkerProperties.Topics("notifications.email"),
                    new WorkerProperties.Consumer("email-worker", 1),
                    new WorkerProperties.Mail("no-reply@example.com"),
                    new WorkerProperties.Delivery(LEASE)),
            Clock.fixed(NOW, ZoneOffset.UTC));

    private final NotificationMessage message = new NotificationMessage(UUID.randomUUID(), "user-1", "EMAIL",
            "bob@example.com", "Hello", "Body text", NOW);

    @Test
    void sendsQueuedNotificationAndMarksItSent() {
        givenQueuedAndClaimable();

        DeliveryOutcome outcome = service.deliver(message);

        assertThat(outcome).isEqualTo(DeliveryOutcome.SENT);
        ArgumentCaptor<SimpleMailMessage> mail = ArgumentCaptor.forClass(SimpleMailMessage.class);
        verify(mailSender).send(mail.capture());
        assertThat(mail.getValue().getFrom()).isEqualTo("no-reply@example.com");
        assertThat(mail.getValue().getTo()).containsExactly("bob@example.com");
        assertThat(mail.getValue().getSubject()).isEqualTo("Hello");
        assertThat(mail.getValue().getText()).isEqualTo("Body text");
        verify(statuses).markSent(message.notificationId(), NOW);
    }

    @Test
    void skipsNotificationThatIsNoLongerQueued() {
        when(statuses.findStatus(message.notificationId())).thenReturn(Optional.of("SENT"));

        assertThat(service.deliver(message)).isEqualTo(DeliveryOutcome.SKIPPED_ALREADY_PROCESSED);

        verify(mailSender, never()).send(any(SimpleMailMessage.class));
        verify(statuses, never()).markSent(any(), any());
    }

    @Test
    void skipsUnknownNotification() {
        when(statuses.findStatus(message.notificationId())).thenReturn(Optional.empty());

        assertThat(service.deliver(message)).isEqualTo(DeliveryOutcome.SKIPPED_UNKNOWN);

        verify(mailSender, never()).send(any(SimpleMailMessage.class));
    }

    @Test
    void marksNotificationFailedWithPublicCodeWhenRecipientIsRejected() {
        givenQueuedAndClaimable();
        doThrow(new MailSendException("Failed messages",
                new SendFailedException("550 5.1.1 mailbox unavailable on smtp.internal:25")))
                .when(mailSender).send(any(SimpleMailMessage.class));

        assertThat(service.deliver(message)).isEqualTo(DeliveryOutcome.FAILED);

        verify(statuses).markFailed(message.notificationId(), EmailDeliveryService.RECIPIENT_REJECTED, NOW);
        verify(statuses, never()).markSent(any(), any());
    }

    @Test
    void throwsTransientErrorAndReleasesClaimWhenMailServerIsUnreachable() {
        givenQueuedAndClaimable();
        doThrow(new MailSendException("Mail server connection failed",
                new MessagingException("Couldn't connect to host, port: smtp.internal, 25")))
                .when(mailSender).send(any(SimpleMailMessage.class));

        assertThatThrownBy(() -> service.deliver(message)).isInstanceOf(TransientDeliveryException.class);

        verify(statuses, never()).markFailed(any(), anyString(), any());
        verify(statuses, never()).markSent(any(), any());
        verify(claims).release(eq(message.notificationId()), anyString());
    }

    @Test
    void throwsTransientErrorWithoutSendingWhenAnotherWorkerHoldsTheClaim() {
        when(statuses.findStatus(message.notificationId())).thenReturn(Optional.of("QUEUED"));
        when(claims.tryClaim(eq(message.notificationId()), anyString(), eq(LEASE))).thenReturn(false);

        assertThatThrownBy(() -> service.deliver(message)).isInstanceOf(TransientDeliveryException.class);

        verify(mailSender, never()).send(any(SimpleMailMessage.class));
        verify(claims, never()).release(any(), anyString());
    }

    @Test
    void skipsWhenPreviousClaimantFinishedJustBeforeClaimWasTakenOver() {
        when(statuses.findStatus(message.notificationId()))
                .thenReturn(Optional.of("QUEUED"))
                .thenReturn(Optional.of("SENT"));
        when(claims.tryClaim(eq(message.notificationId()), anyString(), eq(LEASE))).thenReturn(true);

        assertThat(service.deliver(message)).isEqualTo(DeliveryOutcome.SKIPPED_ALREADY_PROCESSED);

        verify(mailSender, never()).send(any(SimpleMailMessage.class));
    }

    @Test
    void doesNotClaimNotificationThatIsAlreadyFinished() {
        when(statuses.findStatus(message.notificationId())).thenReturn(Optional.of("FAILED"));

        service.deliver(message);

        verify(claims, never()).tryClaim(any(), anyString(), any());
    }

    private void givenQueuedAndClaimable() {
        when(statuses.findStatus(message.notificationId())).thenReturn(Optional.of("QUEUED"));
        when(claims.tryClaim(eq(message.notificationId()), anyString(), eq(LEASE))).thenReturn(true);
    }
}
