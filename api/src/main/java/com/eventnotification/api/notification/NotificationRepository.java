package com.eventnotification.api.notification;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public class NotificationRepository {

    private final JdbcClient jdbc;

    public NotificationRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public void insert(Notification n) {
        jdbc.sql("""
                        INSERT INTO notifications
                            (id, user_id, channel, recipient, subject, body, priority, status, created_at,
                             updated_at)
                        VALUES (:id, :userId, :channel, :recipient, :subject, :body, :priority, :status, :createdAt,
                                :updatedAt)
                        """)
                .param("id", n.id())
                .param("userId", n.userId())
                .param("channel", n.channel().name())
                .param("recipient", n.recipient())
                .param("subject", n.subject())
                .param("body", n.body())
                .param("priority", n.priority().name())
                .param("status", n.status().name())
                .param("createdAt", Timestamp.from(n.createdAt()))
                .param("updatedAt", Timestamp.from(n.updatedAt()))
                .update();
    }

    public Optional<Notification> findById(UUID id) {
        return jdbc.sql("SELECT * FROM notifications WHERE id = :id")
                .param("id", id)
                .query(NotificationRepository::map)
                .optional();
    }

    /**
     * Locks up to {@code limit} DEFERRED notifications whose quiet hours are over, oldest first.
     * SKIP LOCKED lets several API instances release in parallel. Uses the database clock, so the
     * API and the workers that set {@code deliver_after} agree on the time.
     */
    public List<Notification> lockDueDeferred(int limit) {
        return jdbc.sql("""
                        SELECT * FROM notifications
                        WHERE status = 'DEFERRED' AND deliver_after <= now()
                        ORDER BY deliver_after
                        LIMIT :limit
                        FOR UPDATE SKIP LOCKED
                        """)
                .param("limit", limit)
                .query(NotificationRepository::map)
                .list();
    }

    /** Moves DEFERRED back to QUEUED. Returns false if the notification was not DEFERRED. */
    public boolean requeueDeferred(UUID id, Instant now) {
        return jdbc.sql("""
                        UPDATE notifications
                        SET status = 'QUEUED', deliver_after = NULL, updated_at = :now
                        WHERE id = :id AND status = 'DEFERRED'
                        """)
                .param("id", id)
                .param("now", Timestamp.from(now))
                .update() == 1;
    }

    private static Notification map(ResultSet rs, int rowNum) throws SQLException {
        return new Notification(
                rs.getObject("id", UUID.class),
                rs.getString("user_id"),
                Channel.valueOf(rs.getString("channel")),
                rs.getString("recipient"),
                rs.getString("subject"),
                rs.getString("body"),
                Priority.valueOf(rs.getString("priority")),
                NotificationStatus.valueOf(rs.getString("status")),
                rs.getString("failure_reason"),
                toInstant(rs.getTimestamp("created_at")),
                toInstant(rs.getTimestamp("updated_at")),
                toInstant(rs.getTimestamp("sent_at")),
                toInstant(rs.getTimestamp("deliver_after")));
    }

    private static Instant toInstant(Timestamp ts) {
        return ts == null ? null : ts.toInstant();
    }
}
