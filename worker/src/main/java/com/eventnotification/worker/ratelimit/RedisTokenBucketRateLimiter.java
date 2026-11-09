package com.eventnotification.worker.ratelimit;

import com.eventnotification.worker.config.WorkerProperties;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;

import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Per-user, per-channel token buckets in Redis. Every worker shares the same buckets, and the
 * Lua script makes each check-and-take atomic, so two workers handling the same user at the
 * same moment cannot both take the last token.
 *
 * <p>If Redis is unreachable the limiter lets the notification through: a Redis outage should
 * not stop password resets, and the limit exists to protect users from floods, not the system.
 *
 * <p>Each decision is counted in {@code notification_rate_limit_decisions_total{channel,
 * priority, decision}}, where the decision is {@code now}, {@code later}, {@code rejected}, or
 * {@code unavailable} when Redis could not be reached and the notification went through unchecked.
 */
public class RedisTokenBucketRateLimiter implements RateLimiter {

    public static final String DECISIONS = "notification.rate.limit.decisions";

    private static final Logger log = LoggerFactory.getLogger(RedisTokenBucketRateLimiter.class);

    @SuppressWarnings("rawtypes")
    private static final RedisScript<List> SCRIPT =
            RedisScript.of(new ClassPathResource("ratelimit/token-bucket.lua"), List.class);

    private final StringRedisTemplate redis;
    private final Map<String, WorkerProperties.Bucket> buckets;
    private final Map<String, WorkerProperties.Bucket> highPriorityBuckets;
    private final Duration maxWait;
    private final MeterRegistry registry;

    public RedisTokenBucketRateLimiter(StringRedisTemplate redis, WorkerProperties.RateLimit properties,
                                       MeterRegistry registry) {
        this.redis = redis;
        this.registry = registry;
        this.buckets = properties.channels();
        this.highPriorityBuckets = properties.highPriorityChannels();
        this.maxWait = properties.maxWait();
    }

    @Override
    public Decision acquire(String userId, String channel, boolean highPriority) {
        WorkerProperties.Bucket bucket = (highPriority ? highPriorityBuckets : buckets).get(channel);
        if (bucket == null) {
            return Decision.NOW;
        }
        List<?> result;
        try {
            result = redis.execute(SCRIPT, List.of(key(userId, channel, highPriority)),
                    String.valueOf(bucket.capacity()),
                    String.valueOf(bucket.refillPeriod().toMillis()),
                    String.valueOf(maxWait.toMillis()));
        } catch (DataAccessException e) {
            log.warn("Rate limiter unavailable; letting {} notification for user {} through", channel, userId, e);
            count(channel, highPriority, "unavailable");
            return Decision.NOW;
        }
        long status = ((Number) result.get(0)).longValue();
        Duration delay = Duration.ofMillis(((Number) result.get(1)).longValue());
        Decision decision = switch ((int) status) {
            case 1 -> Decision.NOW;
            case 2 -> new Decision(Outcome.LATER, delay);
            default -> new Decision(Outcome.REJECTED, delay);
        };
        count(channel, highPriority, decision.outcome().name().toLowerCase(Locale.ROOT));
        return decision;
    }

    private void count(String channel, boolean highPriority, String decision) {
        Counter.builder(DECISIONS)
                .description("Rate limiter decisions")
                .tag("channel", channel)
                .tag("priority", highPriority ? "HIGH" : "NORMAL")
                .tag("decision", decision)
                .register(registry)
                .increment();
    }

    /** The braces are a Redis Cluster hash tag: all of one user's buckets land on the same node. */
    static String key(String userId, String channel, boolean highPriority) {
        return "ratelimit:{" + userId + "}:" + channel + (highPriority ? ":HIGH" : "");
    }
}
