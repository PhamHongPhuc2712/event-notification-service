package com.eventnotification.api.outbox;

import com.eventnotification.api.TestcontainersConfiguration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.ActiveProfiles;

import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Runs the cleanup's SQL against real Postgres. */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
@ActiveProfiles("test")
class OutboxCleanupIntegrationTest {

    @Autowired
    private OutboxCleaner cleaner;

    @Autowired
    private OutboxRepository outbox;

    @Autowired
    private JdbcClient jdbc;

    @Autowired
    private Clock clock;

    @Test
    void deletesOnlyRowsPublishedBeforeTheRetentionWindow() {
        Instant now = clock.instant();
        UUID oldPublished = insert(now.minus(Duration.ofHours(3)), now.minus(Duration.ofHours(2)));
        UUID recentlyPublished = insert(now.minus(Duration.ofHours(3)), now.minus(Duration.ofMinutes(5)));
        // Old but never published, for example while Kafka was down: it must be kept.
        UUID oldUnpublished = insert(now.minus(Duration.ofHours(3)), null);

        cleaner.deletePublished();

        assertThat(exists(oldPublished)).isFalse();
        assertThat(exists(recentlyPublished)).isTrue();
        assertThat(exists(oldUnpublished)).isTrue();
    }

    @Test
    void deletesAtMostOneBatchPerStatement() {
        Instant now = clock.instant();
        Instant longAgo = now.minus(Duration.ofDays(2));
        UUID first = insert(longAgo, longAgo);
        UUID second = insert(longAgo, longAgo);
        UUID third = insert(longAgo, longAgo);

        int deleted = outbox.deletePublishedBefore(now.minus(Duration.ofDays(1)), 2);

        assertThat(deleted).isEqualTo(2);
        // The oldest rows go first.
        assertThat(exists(first)).isFalse();
        assertThat(exists(second)).isFalse();
        assertThat(exists(third)).isTrue();
    }

    private UUID insert(Instant createdAt, Instant publishedAt) {
        UUID aggregateId = UUID.randomUUID();
        jdbc.sql("""
                        INSERT INTO outbox (aggregate_id, topic, message_key, payload, created_at, published_at)
                        VALUES (:aggregateId, 'outbox-cleanup-test', 'key', '{}'::jsonb, :createdAt, :publishedAt)
                        """)
                .param("aggregateId", aggregateId)
                .param("createdAt", Timestamp.from(createdAt))
                .param("publishedAt", publishedAt == null ? null : Timestamp.from(publishedAt))
                .update();
        return aggregateId;
    }

    private boolean exists(UUID aggregateId) {
        return jdbc.sql("SELECT count(*) FROM outbox WHERE aggregate_id = :id")
                .param("id", aggregateId)
                .query(Long.class)
                .single() > 0;
    }
}
