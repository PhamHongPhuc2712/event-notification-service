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
    void theClaimsAsynchronousCommitDoesNotLeakToOtherStatementsOnThePooledConnection() {
        // Only the claim may skip waiting for the WAL flush; marking SENT must stay durable.
        for (int i = 0; i < 20; i++) {
            claimed(insertNotification(), "worker-a");
            assertThat(jdbc.sql("SHOW synchronous_commit").query(String.class).single()).isEqualTo("on");
        }
    }

    @Test
    void returnsTheNotificationsStateWithTheClaim() {
        UUID id = insertNotification();
        jdbc.sql("UPDATE notifications SET rate_limit_reserved = TRUE WHERE id = :id").param("id", id).update();

        DeliveryClaimRepository.Claim claim = claims.claim(id, "worker-a", LEASE).orElseThrow();

        assertThat(claim).isEqualTo(new DeliveryClaimRepository.Claim(true, "QUEUED", true));
    }

    @Test
    void reportsTheStatusEvenWhenTheClaimIsLost() {
        UUID id = insertNotification();
        claims.claim(id, "worker-a", LEASE);
        jdbc.sql("UPDATE notifications SET status = 'SENT' WHERE id = :id").param("id", id).update();

        assertThat(claims.claim(id, "worker-b", LEASE).orElseThrow())
                .isEqualTo(new DeliveryClaimRepository.Claim(false, "SENT", false));
    }

    @Test
    void claimsNothingForAnUnknownNotification() {
        UUID id = UUID.randomUUID();

        assertThat(claims.claim(id, "worker-a", LEASE)).isEmpty();
        assertThat(jdbc.sql("SELECT count(*) FROM delivery_claims WHERE notification_id = :id")
                .param("id", id).query(Long.class).single()).isZero();
    }

    @Test
    void secondWorkerCannotClaimWhileLeaseIsLive() {
        UUID id = insertNotification();

        assertThat(claimed(id, "worker-a")).isTrue();
        assertThat(claimed(id, "worker-b")).isFalse();
        assertThat(claimant(id)).isEqualTo("worker-a");
    }

    @Test
    void expiredClaimCanBeTakenOver() {
        UUID id = insertNotification();
        claimed(id, "worker-a");
        backdateClaim(id, Duration.ofMinutes(5));

        assertThat(claimed(id, "worker-b")).isTrue();
        assertThat(claimant(id)).isEqualTo("worker-b");
    }

    @Test
    void releasedClaimCanBeClaimedAgain() {
        UUID id = insertNotification();
        claimed(id, "worker-a");

        claims.release(id, "worker-a");

        assertThat(claimed(id, "worker-b")).isTrue();
    }

    @Test
    void workerCannotReleaseAnotherWorkersClaim() {
        UUID id = insertNotification();
        claimed(id, "worker-a");

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
                    return claimed(id, workerId);
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

    private boolean claimed(UUID id, String workerId) {
        return claims.claim(id, workerId, LEASE).orElseThrow().claimed();
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
