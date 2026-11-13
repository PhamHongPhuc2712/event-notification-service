package com.eventnotification.api.outbox;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Repository
public class OutboxRepository {

    private final JdbcClient jdbc;

    public OutboxRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public void insert(UUID aggregateId, String topic, String messageKey, String payload, Instant createdAt) {
        jdbc.sql("""
                        INSERT INTO outbox (aggregate_id, topic, message_key, payload, created_at)
                        VALUES (:aggregateId, :topic, :messageKey, CAST(:payload AS jsonb), :createdAt)
                        """)
                .param("aggregateId", aggregateId)
                .param("topic", topic)
                .param("messageKey", messageKey)
                .param("payload", payload)
                .param("createdAt", Timestamp.from(createdAt))
                .update();
    }

    /**
     * Locks the oldest unpublished rows. SKIP LOCKED lets several publishers run without
     * blocking each other. Must be called inside a transaction.
     */
    public List<OutboxEvent> lockUnpublished(int limit) {
        return jdbc.sql("""
                        SELECT id, aggregate_id, topic, message_key, payload::text AS payload
                        FROM outbox
                        WHERE published_at IS NULL
                        ORDER BY id
                        LIMIT :limit
                        FOR UPDATE SKIP LOCKED
                        """)
                .param("limit", limit)
                .query((rs, rowNum) -> new OutboxEvent(
                        rs.getLong("id"),
                        rs.getObject("aggregate_id", UUID.class),
                        rs.getString("topic"),
                        rs.getString("message_key"),
                        rs.getString("payload")))
                .list();
    }

    /**
     * Deletes up to {@code limit} of the oldest rows published before {@code cutoff} and returns
     * how many it deleted. Ids grow with insertion, so the oldest published rows are at the start
     * of the primary key and no extra index is needed. SKIP LOCKED keeps cleaners on several API
     * instances from waiting on each other.
     */
    public int deletePublishedBefore(Instant cutoff, int limit) {
        return jdbc.sql("""
                        DELETE FROM outbox
                        WHERE id IN (
                            SELECT id FROM outbox
                            WHERE published_at < :cutoff
                            ORDER BY id
                            LIMIT :limit
                            FOR UPDATE SKIP LOCKED)
                        """)
                .param("cutoff", Timestamp.from(cutoff))
                .param("limit", limit)
                .update();
    }

    public void markPublished(List<Long> ids, Instant publishedAt) {
        jdbc.sql("UPDATE outbox SET published_at = :publishedAt WHERE id IN (:ids)")
                .param("publishedAt", Timestamp.from(publishedAt))
                .param("ids", ids)
                .update();
    }
}
