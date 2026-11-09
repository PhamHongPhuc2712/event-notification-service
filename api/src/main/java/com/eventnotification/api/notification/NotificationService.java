package com.eventnotification.api.notification;

import com.eventnotification.api.config.NotificationProperties;
import com.eventnotification.api.idempotency.IdempotencyKey;
import com.eventnotification.api.idempotency.IdempotencyKeyRepository;
import com.eventnotification.api.idempotency.IdempotencyKeyReusedException;
import com.eventnotification.api.outbox.OutboxRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.util.HexFormat;
import java.util.UUID;

@Service
public class NotificationService {

    /** {@code notification_requests_total{channel, priority, result}}, where result is created or replayed. */
    public static final String REQUESTS = "notification.requests";
    /** The log context key that ties log lines to a notification. The worker uses the same one. */
    static final String NOTIFICATION_ID = "notificationId";

    private static final Logger log = LoggerFactory.getLogger(NotificationService.class);

    private final NotificationRepository notifications;
    private final OutboxRepository outbox;
    private final IdempotencyKeyRepository idempotencyKeys;
    private final NotificationProperties properties;
    private final JsonMapper jsonMapper;
    private final MeterRegistry registry;
    private final TransactionTemplate tx;
    private final Clock clock;

    public NotificationService(NotificationRepository notifications, OutboxRepository outbox,
                               IdempotencyKeyRepository idempotencyKeys, NotificationProperties properties,
                               JsonMapper jsonMapper, MeterRegistry registry, TransactionTemplate tx, Clock clock) {
        this.notifications = notifications;
        this.outbox = outbox;
        this.idempotencyKeys = idempotencyKeys;
        this.properties = properties;
        this.jsonMapper = jsonMapper;
        this.registry = registry;
        this.tx = tx;
        this.clock = clock;
    }

    /**
     * Saves the notification and its outbox row in one transaction, so a message is
     * published to Kafka if and only if the notification was stored.
     *
     * <p>With an idempotency key, a repeated request returns the notification the first one
     * created instead of creating another.
     *
     * <p>It is logged and counted only after the transaction commits, so a failed commit is not
     * reported as accepted.
     *
     * @param key the client's idempotency key, or null
     * @throws IdempotencyKeyReusedException if the key was used before with a different body
     */
    public CreateResult create(CreateNotificationRequest request, IdempotencyKey key) {
        CreateResult result = tx.execute(status -> store(request, key));
        Notification notification = result.notification();
        try (MDC.MDCCloseable ignored = MDC.putCloseable(NOTIFICATION_ID, notification.id().toString())) {
            log.info("{} {} notification {} for user {}", result.replayed() ? "Replayed" : "Accepted",
                    notification.channel(), notification.id(), notification.userId());
        }
        Counter.builder(REQUESTS)
                .description("Notifications accepted by the API, and repeats answered from an idempotency key")
                .tag("channel", notification.channel().name())
                .tag("priority", notification.priority().name())
                .tag("result", result.replayed() ? "replayed" : "created")
                .register(registry)
                .increment();
        return result;
    }

    private CreateResult store(CreateNotificationRequest request, IdempotencyKey key) {
        Notification notification = Notification.queued(request, clock.instant());
        if (key != null) {
            String hash = hash(request);
            // Inserted before the notification: a concurrent duplicate blocks here until this commits.
            if (!idempotencyKeys.insertIfAbsent(request.userId(), key, hash, notification.id(),
                    notification.createdAt())) {
                return replay(request.userId(), key, hash);
            }
        }
        notifications.insert(notification);
        publish(notification);
        return new CreateResult(notification, false);
    }

    /**
     * Puts DEFERRED notifications whose quiet hours have ended back on the queue: each is moved to
     * QUEUED and gets a new outbox row, in one transaction.
     *
     * @return how many were released
     */
    @Transactional
    public int releaseDueDeferred(int limit) {
        int released = 0;
        for (Notification deferred : notifications.lockDueDeferred(limit)) {
            if (notifications.requeueDeferred(deferred.id(), clock.instant())) {
                publish(deferred);
                released++;
            }
        }
        return released;
    }

    public Notification get(UUID id) {
        return notifications.findById(id).orElseThrow(() -> new NotificationNotFoundException(id));
    }

    /** Adds the outbox row that the outbox publisher sends to the channel's topic. */
    private void publish(Notification notification) {
        String payload = jsonMapper.writeValueAsString(NotificationMessage.from(notification));
        String topic = notification.priority().topic(topicFor(notification.channel()));
        outbox.insert(notification.id(), topic, notification.userId(), payload,
                clock.instant());
    }

    private CreateResult replay(String userId, IdempotencyKey key, String hash) {
        IdempotencyKeyRepository.StoredKey stored = idempotencyKeys.find(userId, key)
                .orElseThrow(() -> new IllegalStateException("Idempotency key vanished after a conflict"));
        if (!stored.requestHash().equals(hash)) {
            throw new IdempotencyKeyReusedException();
        }
        return new CreateResult(get(stored.notificationId()), true);
    }

    /** SHA-256 of the request fields in a fixed order, so equal requests hash equally. */
    private String hash(CreateNotificationRequest request) {
        Object[] fields = {request.userId(), request.channel().name(), request.recipient(), request.subject(),
                request.body(), request.effectivePriority().name()};
        byte[] canonical = jsonMapper.writeValueAsString(fields).getBytes(StandardCharsets.UTF_8);
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(canonical));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required by every Java runtime", e);
        }
    }

    private String topicFor(Channel channel) {
        return switch (channel) {
            case EMAIL -> properties.topics().email();
            case SMS -> properties.topics().sms();
            case WEBHOOK -> properties.topics().webhook();
        };
    }
}
