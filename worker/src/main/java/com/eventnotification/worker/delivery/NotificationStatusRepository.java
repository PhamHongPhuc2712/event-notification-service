package com.eventnotification.worker.delivery;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

@Repository
public class NotificationStatusRepository {

    private final JdbcClient jdbc;

    public NotificationStatusRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public Optional<String> findStatus(UUID id) {
        return jdbc.sql("SELECT status FROM notifications WHERE id = :id")
                .param("id", id)
                .query(String.class)
                .optional();
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
}
