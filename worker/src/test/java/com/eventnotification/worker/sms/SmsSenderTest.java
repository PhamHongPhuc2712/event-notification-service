package com.eventnotification.worker.sms;

import com.eventnotification.worker.FakeSmsProvider;
import com.eventnotification.worker.config.WorkerProperties;
import com.eventnotification.worker.delivery.FailureCodes;
import com.eventnotification.worker.delivery.NotificationMessage;
import com.eventnotification.worker.delivery.PermanentDeliveryException;
import com.eventnotification.worker.delivery.TransientDeliveryException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.assertThatNoException;

class SmsSenderTest {

    private FakeSmsProvider provider;
    private SmsSender sender;
    private final NotificationMessage message = new NotificationMessage(UUID.randomUUID(), "user-1", "SMS",
            "+6591234567", null, "Your code is 123456", Instant.now());

    @BeforeEach
    void startProvider() throws Exception {
        provider = new FakeSmsProvider();
        sender = new SmsSender(new WorkerProperties(null, null, null,
                new WorkerProperties.Sms(provider.baseUrl(), Duration.ofSeconds(1), Duration.ofSeconds(1)),
                null, null));
    }

    @AfterEach
    void stopProvider() {
        provider.close();
    }

    @Test
    void sendsWithNotificationIdAsIdempotencyKey() {
        assertThatNoException().isThrownBy(() -> sender.send(message));

        assertThat(provider.attempts(message.notificationId().toString())).isEqualTo(1);
    }

    @Test
    void badRequestIsAPermanentRecipientRejection() {
        provider.script(key(), 400);

        assertThatThrownBy(() -> sender.send(message))
                .isInstanceOfSatisfying(PermanentDeliveryException.class,
                        e -> assertThat(e.failureCode()).isEqualTo(FailureCodes.RECIPIENT_REJECTED));
    }

    @Test
    void otherClientErrorsArePermanentProviderRejections() {
        provider.script(key(), 422);

        assertThatThrownBy(() -> sender.send(message))
                .isInstanceOfSatisfying(PermanentDeliveryException.class,
                        e -> assertThat(e.failureCode()).isEqualTo(FailureCodes.REJECTED_BY_PROVIDER));
    }

    @ParameterizedTest
    @ValueSource(ints = {408, 429, 500, 503})
    void throttlingAndServerErrorsAreTransient(int status) {
        provider.script(key(), status);

        assertThatThrownBy(() -> sender.send(message)).isInstanceOf(TransientDeliveryException.class);
    }

    @Test
    void readTimeoutIsTransient() {
        provider.script(key(), FakeSmsProvider.TIMEOUT);

        assertThatThrownBy(() -> sender.send(message)).isInstanceOf(TransientDeliveryException.class);
    }

    @Test
    void unreachableProviderIsTransient() {
        provider.close();

        assertThatThrownBy(() -> sender.send(message)).isInstanceOf(TransientDeliveryException.class);
    }

    private String key() {
        return message.notificationId().toString();
    }
}
