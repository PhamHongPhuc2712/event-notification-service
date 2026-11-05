package com.eventnotification.worker.webhook;

import com.eventnotification.worker.TestWorkerProperties;
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
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WebhookSenderTest {

    private static final Instant NOW = Instant.parse("2026-11-06T09:00:00Z");

    private final JsonMapper jsonMapper = JsonMapper.builder().build();
    private FakeWebhookReceiver receiver;
    private WebhookSender sender;

    @BeforeEach
    void start() throws Exception {
        receiver = new FakeWebhookReceiver();
        sender = sender(true);
    }

    @AfterEach
    void stop() {
        receiver.close();
    }

    @Test
    void postsASignedRequestThatTheReceiverCanVerify() throws Exception {
        NotificationMessage message = message(receiver.url("/hooks/orders"));

        sender.send(message);

        FakeWebhookReceiver.Received request = receiver.received().getFirst();
        assertThat(request.id()).isEqualTo(message.notificationId().toString());
        assertThat(request.timestamp()).isEqualTo(Long.toString(NOW.getEpochSecond()));
        // What a receiver does: recompute the signature over id.timestamp.body with the shared secret.
        String expected = new WebhookSigner(TestWorkerProperties.WEBHOOK_SECRET)
                .sign(request.id(), Long.parseLong(request.timestamp()), request.body());
        assertThat(request.signature()).isEqualTo(expected);

        JsonNode body = jsonMapper.readTree(request.body());
        assertThat(body.get("type").asString()).isEqualTo("order.shipped");
        assertThat(body.get("data").get("notificationId").asString()).isEqualTo(request.id());
        assertThat(body.get("data").get("userId").asString()).isEqualTo("user-1");
        assertThat(body.get("data").get("body").asString()).isEqualTo("{\"orderId\":42}");
    }

    @Test
    void retriesKeepTheIdButSignAgain() throws Exception {
        NotificationMessage message = message(receiver.url("/hook"));
        sender.send(message);
        sender(true, Clock.fixed(NOW.plusSeconds(30), ZoneOffset.UTC)).send(message);

        var first = receiver.received().get(0);
        var second = receiver.received().get(1);
        assertThat(second.id()).isEqualTo(first.id());
        assertThat(second.timestamp()).isNotEqualTo(first.timestamp());
        assertThat(second.signature()).isNotEqualTo(first.signature());
    }

    @ParameterizedTest
    @ValueSource(ints = {408, 429, 500, 502, 503})
    void throttlingAndServerErrorsAreTransient(int status) {
        receiver.respondWith(status);

        assertThatThrownBy(() -> sender.send(message(receiver.url("/hook"))))
                .isInstanceOf(TransientDeliveryException.class);
    }

    @ParameterizedTest
    @ValueSource(ints = {400, 401, 404, 410})
    void otherClientErrorsArePermanent(int status) {
        receiver.respondWith(status);

        assertThatThrownBy(() -> sender.send(message(receiver.url("/hook"))))
                .isInstanceOfSatisfying(PermanentDeliveryException.class,
                        e -> assertThat(e.failureCode()).isEqualTo(FailureCodes.REJECTED_BY_PROVIDER));
    }

    @Test
    void redirectsAreNotFollowed() {
        receiver.respondWith(302, Map.of("Location", "http://169.254.169.254/latest/meta-data/"));

        assertThatThrownBy(() -> sender.send(message(receiver.url("/hook"))))
                .isInstanceOf(PermanentDeliveryException.class);
        assertThat(receiver.received()).hasSize(1);
    }

    @Test
    void slowReceiverTimesOutAsTransient() {
        receiver.delay(Duration.ofMillis(1_500));

        assertThatThrownBy(() -> sender.send(message(receiver.url("/hook"))))
                .isInstanceOf(TransientDeliveryException.class);
    }

    @Test
    void privateTargetIsRejectedBeforeAnyRequestWhenNotAllowed() {
        WebhookSender strict = sender(false);

        assertThatThrownBy(() -> strict.send(message(receiver.url("/hook"))))
                .isInstanceOfSatisfying(PermanentDeliveryException.class,
                        e -> assertThat(e.failureCode()).isEqualTo(FailureCodes.TARGET_NOT_ALLOWED));
        assertThat(receiver.received()).isEmpty();
    }

    private WebhookSender sender(boolean allowPrivate) {
        return sender(allowPrivate, Clock.fixed(NOW, ZoneOffset.UTC));
    }

    private WebhookSender sender(boolean allowPrivate, Clock clock) {
        WorkerProperties base = TestWorkerProperties.create();
        WorkerProperties properties = new WorkerProperties(base.topics(), base.consumer(), base.mail(), base.sms(),
                TestWorkerProperties.webhook(allowPrivate), base.delivery(), base.retry(), base.rateLimit());
        return new WebhookSender(properties, jsonMapper, clock);
    }

    private static NotificationMessage message(String url) {
        return new NotificationMessage(UUID.randomUUID(), "user-1", "WEBHOOK", url, "order.shipped",
                "{\"orderId\":42}", NOW);
    }
}
