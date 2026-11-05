package com.eventnotification.worker.ratelimit;

import com.eventnotification.worker.config.WorkerProperties;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;

@Configuration(proxyBeanMethods = false)
class RateLimitConfig {

    /** With rate limiting off, every notification is sent at once and Redis is never contacted. */
    @Bean
    RateLimiter rateLimiter(WorkerProperties properties, ObjectProvider<StringRedisTemplate> redis) {
        WorkerProperties.RateLimit rateLimit = properties.rateLimit();
        if (!rateLimit.enabled()) {
            return (userId, channel, highPriority) -> RateLimiter.Decision.NOW;
        }
        return new RedisTokenBucketRateLimiter(redis.getObject(), rateLimit);
    }
}
