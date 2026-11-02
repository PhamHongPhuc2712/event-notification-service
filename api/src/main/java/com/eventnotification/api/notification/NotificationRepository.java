package com.eventnotification.api.notification;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
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
                            (id, user_id, channel, recipient, subject, body, status, created_at, updated_at)
                        VALUES (:id, :userId, :channel, :recipient, :subject, :body, :status, :createdAt, :updatedAt)
                        """)
                .param("id", n.id())
                .param("userId", n.userId())
                .param("channel", n.channel().name())
                .param("recipient", n.recipient())
                .param("subject", n.subject())
                .param("body", n.body())
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

    private static Notification map(ResultSet rs, int rowNum) throws SQLException {
        return new Notification(
                rs.getObject("id", UUID.class),
                rs.getString("user_id"),
                Channel.valueOf(rs.getString("channel")),
                rs.getString("recipient"),
                rs.getString("subject"),
                rs.getString("body"),
                NotificationStatus.valueOf(rs.getString("status")),
                rs.getString("failure_reason"),
                toInstant(rs.getTimestamp("created_at")),
                toInstant(rs.getTimestamp("updated_at")),
                toInstant(rs.getTimestamp("sent_at")));
    }

    private static Instant toInstant(Timestamp ts) {
        return ts == null ? null : ts.toInstant();
    }
}
