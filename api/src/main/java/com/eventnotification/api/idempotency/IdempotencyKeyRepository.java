package com.eventnotification.api.idempotency;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

@Repository
public class IdempotencyKeyRepository {

    public record StoredKey(String requestHash, UUID notificationId) {
    }

    private final JdbcClient jdbc;

    public IdempotencyKeyRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Records the key for a new notification. If another transaction is inserting the same key,
     * this waits until it commits or rolls back.
     *
     * @return true if the key was new; false if it was already used
     */
    public boolean insertIfAbsent(String userId, IdempotencyKey key, String requestHash, UUID notificationId,
                                  Instant now) {
        return jdbc.sql("""
                        INSERT INTO idempotency_keys (user_id, idempotency_key, request_hash, notification_id, created_at)
                        VALUES (:userId, :key, :hash, :notificationId, :now)
                        ON CONFLICT (user_id, idempotency_key) DO NOTHING
                        """)
                .param("userId", userId)
                .param("key", key.value())
                .param("hash", requestHash)
                .param("notificationId", notificationId)
                .param("now", Timestamp.from(now))
                .update() == 1;
    }

    public Optional<StoredKey> find(String userId, IdempotencyKey key) {
        return jdbc.sql("""
                        SELECT request_hash, notification_id FROM idempotency_keys
                        WHERE user_id = :userId AND idempotency_key = :key
                        """)
                .param("userId", userId)
                .param("key", key.value())
                .query((rs, rowNum) -> new StoredKey(rs.getString("request_hash"),
                        rs.getObject("notification_id", UUID.class)))
                .optional();
    }
}
