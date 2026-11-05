package com.eventnotification.api.notification;

import com.eventnotification.api.config.NotificationProperties;
import com.eventnotification.api.idempotency.IdempotencyKey;
import com.eventnotification.api.idempotency.IdempotencyKeyRepository;
import com.eventnotification.api.idempotency.IdempotencyKeyReusedException;
import com.eventnotification.api.outbox.OutboxRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.util.HexFormat;
import java.util.UUID;

@Service
public class NotificationService {

    private final NotificationRepository notifications;
    private final OutboxRepository outbox;
    private final IdempotencyKeyRepository idempotencyKeys;
    private final NotificationProperties properties;
    private final JsonMapper jsonMapper;
    private final Clock clock;

    public NotificationService(NotificationRepository notifications, OutboxRepository outbox,
                               IdempotencyKeyRepository idempotencyKeys, NotificationProperties properties,
                               JsonMapper jsonMapper, Clock clock) {
        this.notifications = notifications;
        this.outbox = outbox;
        this.idempotencyKeys = idempotencyKeys;
        this.properties = properties;
        this.jsonMapper = jsonMapper;
        this.clock = clock;
    }

    /**
     * Saves the notification and its outbox row in one transaction, so a message is
     * published to Kafka if and only if the notification was stored.
     *
     * <p>With an idempotency key, a repeated request returns the notification the first one
     * created instead of creating another.
     *
     * @param key the client's idempotency key, or null
     * @throws IdempotencyKeyReusedException if the key was used before with a different body
     */
    @Transactional
    public CreateResult create(CreateNotificationRequest request, IdempotencyKey key) {
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
        outbox.insert(notification.id(), topicFor(notification.channel()), notification.userId(), payload,
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
                request.body()};
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
        };
    }
}
