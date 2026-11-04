package com.eventnotification.worker.delivery;

import com.eventnotification.worker.TestcontainersConfiguration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.ActiveProfiles;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;

/** Claims against real Postgres, because the guarantee comes from its primary key. */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
@ActiveProfiles("test")
class DeliveryClaimRepositoryTest {

    private static final Duration LEASE = Duration.ofSeconds(60);

    @Autowired
    private DeliveryClaimRepository claims;

    @Autowired
    private JdbcClient jdbc;

    @Test
    void secondWorkerCannotClaimWhileLeaseIsLive() {
        UUID id = insertNotification();

        assertThat(claims.tryClaim(id, "worker-a", LEASE)).isTrue();
        assertThat(claims.tryClaim(id, "worker-b", LEASE)).isFalse();
        assertThat(claimant(id)).isEqualTo("worker-a");
    }

    @Test
    void expiredClaimCanBeTakenOver() {
        UUID id = insertNotification();
        claims.tryClaim(id, "worker-a", LEASE);
        backdateClaim(id, Duration.ofMinutes(5));

        assertThat(claims.tryClaim(id, "worker-b", LEASE)).isTrue();
        assertThat(claimant(id)).isEqualTo("worker-b");
    }

    @Test
    void releasedClaimCanBeClaimedAgain() {
        UUID id = insertNotification();
        claims.tryClaim(id, "worker-a", LEASE);

        claims.release(id, "worker-a");

        assertThat(claims.tryClaim(id, "worker-b", LEASE)).isTrue();
    }

    @Test
    void workerCannotReleaseAnotherWorkersClaim() {
        UUID id = insertNotification();
        claims.tryClaim(id, "worker-a", LEASE);

        claims.release(id, "worker-b");

        assertThat(claimant(id)).isEqualTo("worker-a");
    }

    @Test
    void exactlyOneOfManyConcurrentWorkersWinsTheClaim() throws Exception {
        UUID id = insertNotification();
        int workers = 16;
        CountDownLatch start = new CountDownLatch(1);
        List<Future<Boolean>> results = new ArrayList<>();

        try (ExecutorService pool = Executors.newFixedThreadPool(workers)) {
            for (int i = 0; i < workers; i++) {
                String workerId = "worker-" + i;
                results.add(pool.submit(() -> {
                    start.await();
                    return claims.tryClaim(id, workerId, LEASE);
                }));
            }
            start.countDown();

            int wins = 0;
            for (Future<Boolean> result : results) {
                wins += result.get() ? 1 : 0;
            }
            assertThat(wins).isEqualTo(1);
        }
    }

    private UUID insertNotification() {
        UUID id = UUID.randomUUID();
        Timestamp now = Timestamp.from(Instant.now());
        jdbc.sql("""
                        INSERT INTO notifications
                            (id, user_id, channel, recipient, subject, body, status, created_at, updated_at)
                        VALUES (:id, 'user-1', 'EMAIL', 'bob@example.com', 'Hi', 'Body', 'QUEUED', :now, :now)
                        """)
                .param("id", id)
                .param("now", now)
                .update();
        return id;
    }

    private void backdateClaim(UUID id, Duration age) {
        jdbc.sql("UPDATE delivery_claims SET claimed_at = now() - make_interval(secs => :secs) WHERE notification_id = :id")
                .param("secs", (double) age.toSeconds())
                .param("id", id)
                .update();
    }

    private String claimant(UUID id) {
        return jdbc.sql("SELECT claimed_by FROM delivery_claims WHERE notification_id = :id")
                .param("id", id)
                .query(String.class)
                .single();
    }
}
