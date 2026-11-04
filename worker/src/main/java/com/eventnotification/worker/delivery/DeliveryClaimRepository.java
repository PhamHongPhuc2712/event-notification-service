package com.eventnotification.worker.delivery;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.time.Duration;
import java.util.UUID;

/**
 * Claims on notifications, so that only one worker sends each one.
 *
 * <p>Lease times use the database clock, so workers with skewed clocks still agree on
 * whether a claim has expired.
 */
@Repository
public class DeliveryClaimRepository {

    private final JdbcClient jdbc;

    public DeliveryClaimRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Claims the notification for this worker. Succeeds if nobody holds a claim or the
     * existing claim is older than the lease. Concurrent callers wait on the primary key,
     * so at most one of them succeeds.
     *
     * @return true if this worker now holds the claim
     */
    public boolean tryClaim(UUID notificationId, String workerId, Duration lease) {
        return jdbc.sql("""
                        INSERT INTO delivery_claims (notification_id, claimed_by, claimed_at)
                        VALUES (:id, :workerId, now())
                        ON CONFLICT (notification_id) DO UPDATE
                            SET claimed_by = EXCLUDED.claimed_by, claimed_at = EXCLUDED.claimed_at
                            WHERE delivery_claims.claimed_at < now() - make_interval(secs => :leaseSeconds)
                        """)
                .param("id", notificationId)
                .param("workerId", workerId)
                .param("leaseSeconds", lease.toMillis() / 1000.0)
                .update() == 1;
    }

    /** Gives up this worker's claim so a retry can claim it again straight away. */
    public void release(UUID notificationId, String workerId) {
        jdbc.sql("DELETE FROM delivery_claims WHERE notification_id = :id AND claimed_by = :workerId")
                .param("id", notificationId)
                .param("workerId", workerId)
                .update();
    }
}
