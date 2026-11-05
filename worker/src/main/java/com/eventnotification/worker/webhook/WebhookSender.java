package com.eventnotification.worker.webhook;

import com.eventnotification.worker.config.WorkerProperties;
import com.eventnotification.worker.delivery.ChannelSender;
import com.eventnotification.worker.delivery.FailureCodes;
import com.eventnotification.worker.delivery.NotificationMessage;
import com.eventnotification.worker.delivery.PermanentDeliveryException;
import com.eventnotification.worker.delivery.TransientDeliveryException;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Clock;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * POSTs a notification to the recipient URL, signed per the Standard Webhooks spec.
 *
 * <p>{@code webhook-id} is the notification ID and stays the same on every retry, so a
 * receiver that stores the IDs it has handled ignores repeats. {@code webhook-timestamp} is the
 * time of this attempt, so each retry carries a fresh signature.
 */
@Component
public class WebhookSender implements ChannelSender {

    static final String ID_HEADER = "webhook-id";
    static final String TIMESTAMP_HEADER = "webhook-timestamp";
    static final String SIGNATURE_HEADER = "webhook-signature";

    private final HttpClient client;
    private final WebhookSigner signer;
    private final WebhookTargetGuard guard;
    private final Duration readTimeout;
    private final JsonMapper jsonMapper;
    private final Clock clock;

    public WebhookSender(WorkerProperties properties, JsonMapper jsonMapper, Clock clock) {
        WorkerProperties.Webhook webhook = properties.webhook();
        // Redirects are not followed: a 302 to an internal address would skip the guard.
        this.client = HttpClient.newBuilder()
                .connectTimeout(webhook.connectTimeout())
                .followRedirects(HttpClient.Redirect.NEVER)
                .build();
        this.signer = new WebhookSigner(webhook.signingSecret());
        this.guard = new WebhookTargetGuard(webhook.allowPrivateTargets());
        this.readTimeout = webhook.readTimeout();
        this.jsonMapper = jsonMapper;
        this.clock = clock;
    }

    @Override
    public void send(NotificationMessage message) {
        URI target = guard.check(message.recipient());
        String id = message.notificationId().toString();
        String body = jsonMapper.writeValueAsString(payload(message));
        long timestamp = clock.instant().getEpochSecond();
        HttpRequest request = HttpRequest.newBuilder(target)
                .timeout(readTimeout)
                .header("Content-Type", "application/json")
                .header("User-Agent", "event-notification-service")
                .header(ID_HEADER, id)
                .header(TIMESTAMP_HEADER, Long.toString(timestamp))
                .header(SIGNATURE_HEADER, signer.sign(id, timestamp, body))
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();

        int status;
        try {
            status = client.send(request, HttpResponse.BodyHandlers.discarding()).statusCode();
        } catch (IOException e) {
            // Connection refused, reset, TLS failure or timeout.
            throw new TransientDeliveryException("Webhook for notification " + id + " could not be delivered", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TransientDeliveryException("Interrupted delivering webhook for notification " + id, e);
        }
        classify(status, id);
    }

    /** 2xx is delivered; 408, 429 and 5xx are retried; redirects and other 4xx are final. */
    private static void classify(int status, String id) {
        if (status >= 200 && status < 300) {
            return;
        }
        if (status == 408 || status == 429 || status >= 500) {
            throw new TransientDeliveryException("Webhook receiver answered " + status + " for notification " + id,
                    null);
        }
        throw new PermanentDeliveryException(FailureCodes.REJECTED_BY_PROVIDER,
                "Webhook receiver answered " + status + " for notification " + id, null);
    }

    /** The Standard Webhooks payload shape: an event type, a timestamp and the data. */
    private static Map<String, Object> payload(NotificationMessage message) {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("notificationId", message.notificationId());
        data.put("userId", message.userId());
        data.put("body", message.body());
        data.put("priority", message.priority());
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("type", message.subject() == null || message.subject().isBlank() ? "notification"
                : message.subject());
        payload.put("timestamp", message.createdAt());
        payload.put("data", data);
        return payload;
    }
}
