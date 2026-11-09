package com.eventnotification.worker.delivery;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;

/**
 * Claims on notifications, so that only one worker sends each one.
 *
 * <p>Lease times use the database clock, so workers with skewed clocks still agree on
 * whether a claim has expired.
 *
 * <p>Claims commit asynchronously: Postgres acknowledges the commit before flushing it to disk.
 * Under load every synchronous commit waits for a WAL flush, and the flush rate of the disk was
 * the system's limit. It is safe because WAL is written in order: marking the notification SENT
 * commits synchronously and flushes the claim with it, so a database crash can lose a claim only
 * if the notification was not marked SENT, and then Kafka delivers the message again anyway.
 */
@Repository
public class DeliveryClaimRepository {

    /**
     * The result of a claim attempt, together with the notification's state, read in the same
     * statement so a delivery needs one round trip to Postgres instead of four.
     *
     * @param claimed           true if this worker now holds the claim
     * @param status            the notification's status, such as QUEUED or SENT
     * @param rateLimitReserved true if the notification already holds a rate-limit slot
     */
    public record Claim(boolean claimed, String status, boolean rateLimitReserved) {
    }

    private final JdbcClient jdbc;
    private final TransactionTemplate tx;

    public DeliveryClaimRepository(JdbcClient jdbc, TransactionTemplate tx) {
        this.jdbc = jdbc;
        this.tx = tx;
    }

    /**
     * Claims the notification for this worker and reads its state. The claim succeeds if nobody
     * holds one or the existing one is older than the lease. Concurrent callers wait on the
     * primary key, so at most one of them succeeds.
     *
     * <p>The state is read in the same statement, so it can be at most as stale as a separate
     * read straight after the claim would be: a previous claimant whose lease expired may still
     * finish at any moment, which is the documented resend window.
     *
     * @return empty if no such notification exists; nothing is claimed then
     */
    public Optional<Claim> claim(UUID notificationId, String workerId, Duration lease) {
        return tx.execute(status -> {
            // SET LOCAL lasts until the end of this transaction only, not the pooled connection.
            jdbc.sql("SET LOCAL synchronous_commit TO OFF").update();
            return readAndClaim(notificationId, workerId, lease);
        });
    }

    private Optional<Claim> readAndClaim(UUID notificationId, String workerId, Duration lease) {
        return jdbc.sql("""
                        WITH claim AS (
                            INSERT INTO delivery_claims (notification_id, claimed_by, claimed_at)
                            SELECT :id, :workerId, now()
                            WHERE EXISTS (SELECT 1 FROM notifications WHERE id = :id)
                            ON CONFLICT (notification_id) DO UPDATE
                                SET claimed_by = EXCLUDED.claimed_by, claimed_at = EXCLUDED.claimed_at
                                WHERE delivery_claims.claimed_at < now() - make_interval(secs => :leaseSeconds)
                            RETURNING notification_id
                        )
                        SELECT n.status, n.rate_limit_reserved, EXISTS (SELECT 1 FROM claim) AS claimed
                        FROM notifications n
                        WHERE n.id = :id
                        """)
                .param("id", notificationId)
                .param("workerId", workerId)
                .param("leaseSeconds", lease.toMillis() / 1000.0)
                .query((rs, rowNum) -> new Claim(rs.getBoolean("claimed"), rs.getString("status"),
                        rs.getBoolean("rate_limit_reserved")))
                .optional();
    }

    /** Gives up this worker's claim so a retry can claim it again straight away. */
    public void release(UUID notificationId, String workerId) {
        jdbc.sql("DELETE FROM delivery_claims WHERE notification_id = :id AND claimed_by = :workerId")
                .param("id", notificationId)
                .param("workerId", workerId)
                .update();
    }
}
