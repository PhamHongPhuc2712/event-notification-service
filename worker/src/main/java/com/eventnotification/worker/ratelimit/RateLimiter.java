package com.eventnotification.worker.ratelimit;

import java.time.Duration;

/** Limits how many notifications one user receives per channel. */
public interface RateLimiter {

    enum Outcome {
        /** Send now; a token was taken. */
        NOW,
        /** A slot was reserved {@code delay} from now; send then without asking again. */
        LATER,
        /** The next slot is too far away; nothing was reserved. */
        REJECTED
    }

    record Decision(Outcome outcome, Duration delay) {

        public static final Decision NOW = new Decision(Outcome.NOW, Duration.ZERO);
    }

    /**
     * @param highPriority HIGH notifications draw from a separate, smaller bucket, so that
     *                     marketing traffic cannot use up the tokens a password reset needs
     */
    Decision acquire(String userId, String channel, boolean highPriority);

    default Decision acquire(String userId, String channel) {
        return acquire(userId, channel, false);
    }
}
