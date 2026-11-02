package com.eventnotification.api.notification;

import com.eventnotification.api.config.NotificationProperties;
import com.eventnotification.api.outbox.OutboxRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.json.JsonMapper;

import java.time.Clock;
import java.util.UUID;

@Service
public class NotificationService {

    private final NotificationRepository notifications;
    private final OutboxRepository outbox;
    private final NotificationProperties properties;
    private final JsonMapper jsonMapper;
    private final Clock clock;

    public NotificationService(NotificationRepository notifications, OutboxRepository outbox,
                               NotificationProperties properties, JsonMapper jsonMapper, Clock clock) {
        this.notifications = notifications;
        this.outbox = outbox;
        this.properties = properties;
        this.jsonMapper = jsonMapper;
        this.clock = clock;
    }

    /**
     * Saves the notification and its outbox row in one transaction, so a message is
     * published to Kafka if and only if the notification was stored.
     */
    @Transactional
    public Notification create(CreateNotificationRequest request) {
        Notification notification = Notification.queued(request, clock.instant());
        notifications.insert(notification);
        String payload = jsonMapper.writeValueAsString(NotificationMessage.from(notification));
        outbox.insert(notification.id(), topicFor(notification.channel()), notification.userId(), payload,
                notification.createdAt());
        return notification;
    }

    public Notification get(UUID id) {
        return notifications.findById(id).orElseThrow(() -> new NotificationNotFoundException(id));
    }

    private String topicFor(Channel channel) {
        return switch (channel) {
            case EMAIL -> properties.topics().email();
        };
    }
}
