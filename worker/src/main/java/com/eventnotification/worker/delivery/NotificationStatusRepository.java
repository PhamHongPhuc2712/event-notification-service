package com.eventnotification.worker.delivery;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;

@Repository
public class NotificationStatusRepository {

    private final JdbcClient jdbc;

    public NotificationStatusRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** Moves QUEUED to SENT. Returns false if the notification was not QUEUED. */
    public boolean markSent(UUID id, Instant now) {
        return jdbc.sql("""
                        UPDATE notifications
                        SET status = 'SENT', sent_at = :now, updated_at = :now
                        WHERE id = :id AND status = 'QUEUED'
                        """)
                .param("id", id)
                .param("now", Timestamp.from(now))
                .update() == 1;
    }

    /**
     * Moves a dead-lettered notification back to QUEUED so a replayed message can send it.
     * Only notifications that ran out of retries are requeued; a rejected recipient stays FAILED.
     */
    public boolean requeueExhausted(UUID id, Instant now) {
        return jdbc.sql("""
                        UPDATE notifications
                        SET status = 'QUEUED', failure_reason = NULL, updated_at = :now
                        WHERE id = :id AND status = 'FAILED' AND failure_reason = :reason
                        """)
                .param("id", id)
                .param("reason", FailureCodes.RETRIES_EXHAUSTED)
                .param("now", Timestamp.from(now))
                .update() == 1;
    }

    /** Moves QUEUED to FAILED. Returns false if the notification was not QUEUED. */
    public boolean markFailed(UUID id, String reason, Instant now) {
        return jdbc.sql("""
                        UPDATE notifications
                        SET status = 'FAILED', failure_reason = :reason, updated_at = :now
                        WHERE id = :id AND status = 'QUEUED'
                        """)
                .param("id", id)
                .param("reason", reason)
                .param("now", Timestamp.from(now))
                .update() == 1;
    }

    /** Records that this notification holds a rate-limit slot, for example before a send is retried. */
    public void markRateLimitSlotTaken(UUID id) {
        jdbc.sql("UPDATE notifications SET rate_limit_reserved = TRUE WHERE id = :id")
                .param("id", id)
                .update();
    }

    /**
     * Moves QUEUED to DEFERRED until a reserved rate-limit slot, and records the reservation so
     * the released notification does not take a second slot.
     */
    public boolean markDeferredForRateLimit(UUID id, Instant deliverAfter, Instant now) {
        return jdbc.sql("""
                        UPDATE notifications
                        SET status = 'DEFERRED', deliver_after = :deliverAfter, rate_limit_reserved = TRUE,
                            updated_at = :now
                        WHERE id = :id AND status = 'QUEUED'
                        """)
                .param("id", id)
                .param("deliverAfter", Timestamp.from(deliverAfter))
                .param("now", Timestamp.from(now))
                .update() == 1;
    }

    /** Moves QUEUED to SUPPRESSED, with the reason. Returns false if the notification was not QUEUED. */
    public boolean markSuppressed(UUID id, String reason, Instant now) {
        return jdbc.sql("""
                        UPDATE notifications
                        SET status = 'SUPPRESSED', failure_reason = :reason, updated_at = :now
                        WHERE id = :id AND status = 'QUEUED'
                        """)
                .param("id", id)
                .param("reason", reason)
                .param("now", Timestamp.from(now))
                .update() == 1;
    }

    /**
     * Moves QUEUED to DEFERRED until {@code deliverAfter}, when the api module queues it again.
     * Returns false if the notification was not QUEUED.
     */
    public boolean markDeferred(UUID id, Instant deliverAfter, Instant now) {
        return jdbc.sql("""
                        UPDATE notifications
                        SET status = 'DEFERRED', deliver_after = :deliverAfter, updated_at = :now
                        WHERE id = :id AND status = 'QUEUED'
                        """)
                .param("id", id)
                .param("deliverAfter", Timestamp.from(deliverAfter))
                .param("now", Timestamp.from(now))
                .update() == 1;
    }
}
