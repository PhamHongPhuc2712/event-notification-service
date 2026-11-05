package com.eventnotification.api.notification;

import com.eventnotification.api.config.NotificationProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Releases notifications that a worker deferred for quiet hours, once the quiet hours are over.
 * The worker checks the preferences again when the released message arrives.
 */
@Component
class DeferredNotificationReleaser {

    private static final Logger log = LoggerFactory.getLogger(DeferredNotificationReleaser.class);

    private final NotificationService service;
    private final int batchSize;

    DeferredNotificationReleaser(NotificationService service, NotificationProperties properties) {
        this.service = service;
        this.batchSize = properties.deferred().batchSize();
    }

    @Scheduled(fixedDelayString = "${notifications.deferred.poll-interval}")
    void releaseDue() {
        int released;
        do {
            released = service.releaseDueDeferred(batchSize);
            if (released > 0) {
                log.info("Released {} notifications after quiet hours", released);
            }
        } while (released == batchSize);
    }
}
