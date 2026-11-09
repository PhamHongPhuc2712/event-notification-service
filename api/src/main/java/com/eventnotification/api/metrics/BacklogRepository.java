package com.eventnotification.api.metrics;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.time.Duration;

/**
 * Reads how much work is waiting. Only unpublished outbox rows and DEFERRED notifications are
 * visited, through partial indexes, so the cost follows the backlog, not the size of the tables.
 */
@Repository
public class BacklogRepository {

    /**
     * @param outboxRows      outbox rows not yet published to Kafka
     * @param oldestOutboxAge how long the oldest of them has waited; zero if there are none
     * @param deferred        notifications held for quiet hours or a rate-limit slot
     */
    public record Backlog(long outboxRows, Duration oldestOutboxAge, long deferred) {
    }

    private final JdbcClient jdbc;

    public BacklogRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public Backlog read() {
        return jdbc.sql("""
                        SELECT (SELECT count(*) FROM outbox WHERE published_at IS NULL) AS outbox_rows,
                               -- The lowest unpublished id is the oldest row: a single index lookup.
                               CAST(COALESCE(EXTRACT(EPOCH FROM now() - (
                                   SELECT created_at FROM outbox WHERE published_at IS NULL ORDER BY id LIMIT 1
                               )) * 1000, 0) AS BIGINT) AS oldest_ms,
                               (SELECT count(*) FROM notifications WHERE status = 'DEFERRED') AS deferred
                        """)
                .query((rs, rowNum) -> new Backlog(rs.getLong("outbox_rows"),
                        Duration.ofMillis(Math.max(0, rs.getLong("oldest_ms"))), rs.getLong("deferred")))
                .single();
    }
}
