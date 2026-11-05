package com.eventnotification.worker.ratelimit;

import com.eventnotification.worker.config.WorkerProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Per-user, per-channel token buckets in Redis. Every worker shares the same buckets, and the
 * Lua script makes each check-and-take atomic, so two workers handling the same user at the
 * same moment cannot both take the last token.
 *
 * <p>If Redis is unreachable the limiter lets the notification through: a Redis outage should
 * not stop password resets, and the limit exists to protect users from floods, not the system.
 */
public class RedisTokenBucketRateLimiter implements RateLimiter {

    private static final Logger log = LoggerFactory.getLogger(RedisTokenBucketRateLimiter.class);

    @SuppressWarnings("rawtypes")
    private static final RedisScript<List> SCRIPT =
            RedisScript.of(new ClassPathResource("ratelimit/token-bucket.lua"), List.class);

    private final StringRedisTemplate redis;
    private final Map<String, WorkerProperties.Bucket> buckets;
    private final Duration maxWait;

    public RedisTokenBucketRateLimiter(StringRedisTemplate redis, WorkerProperties.RateLimit properties) {
        this.redis = redis;
        this.buckets = properties.channels();
        this.maxWait = properties.maxWait();
    }

    @Override
    public Decision acquire(String userId, String channel) {
        WorkerProperties.Bucket bucket = buckets.get(channel);
        if (bucket == null) {
            return Decision.NOW;
        }
        List<?> result;
        try {
            result = redis.execute(SCRIPT, List.of(key(userId, channel)),
                    String.valueOf(bucket.capacity()),
                    String.valueOf(bucket.refillPeriod().toMillis()),
                    String.valueOf(maxWait.toMillis()));
        } catch (DataAccessException e) {
            log.warn("Rate limiter unavailable; letting {} notification for user {} through", channel, userId, e);
            return Decision.NOW;
        }
        long status = ((Number) result.get(0)).longValue();
        Duration delay = Duration.ofMillis(((Number) result.get(1)).longValue());
        return switch ((int) status) {
            case 1 -> Decision.NOW;
            case 2 -> new Decision(Outcome.LATER, delay);
            default -> new Decision(Outcome.REJECTED, delay);
        };
    }

    /** The braces are a Redis Cluster hash tag: all of one user's buckets land on the same node. */
    static String key(String userId, String channel) {
        return "ratelimit:{" + userId + "}:" + channel;
    }
}
