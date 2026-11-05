package com.eventnotification.worker.ratelimit;

import com.eventnotification.worker.TestcontainersConfiguration;
import com.eventnotification.worker.config.WorkerProperties;
import com.eventnotification.worker.ratelimit.RateLimiter.Decision;
import com.eventnotification.worker.ratelimit.RateLimiter.Outcome;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.ActiveProfiles;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;

/** The Lua token bucket against real Redis, including many workers hitting one user at once. */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
@ActiveProfiles("test")
class RedisTokenBucketRateLimiterTest {

    private static final Duration TOLERANCE = Duration.ofMillis(150);

    @Autowired
    private StringRedisTemplate redis;

    @Test
    void allowsABurstUpToCapacityThenReservesSlotsAtTheRefillRate() {
        RateLimiter limiter = limiter(3, Duration.ofSeconds(1), Duration.ofHours(1));
        String user = uniqueUser();

        for (int i = 0; i < 3; i++) {
            assertThat(limiter.acquire(user, "SMS")).isEqualTo(Decision.NOW);
        }
        for (int slot = 1; slot <= 3; slot++) {
            Decision decision = limiter.acquire(user, "SMS");
            assertThat(decision.outcome()).isEqualTo(Outcome.LATER);
            assertThat(decision.delay()).isCloseTo(Duration.ofSeconds(slot), TOLERANCE);
        }
    }

    @Test
    void refillsOverTime() throws Exception {
        RateLimiter limiter = limiter(1, Duration.ofMillis(200), Duration.ofHours(1));
        String user = uniqueUser();
        assertThat(limiter.acquire(user, "SMS")).isEqualTo(Decision.NOW);

        Thread.sleep(250);

        assertThat(limiter.acquire(user, "SMS")).isEqualTo(Decision.NOW);
    }

    @Test
    void rejectsWithoutTakingASlotWhenTheWaitIsTooLong() {
        RateLimiter limiter = limiter(1, Duration.ofSeconds(10), Duration.ofSeconds(5));
        String user = uniqueUser();
        limiter.acquire(user, "SMS");

        Decision first = limiter.acquire(user, "SMS");
        Decision second = limiter.acquire(user, "SMS");

        assertThat(first.outcome()).isEqualTo(Outcome.REJECTED);
        // A rejection reserves nothing, so the next caller is not pushed further back.
        assertThat(second.outcome()).isEqualTo(Outcome.REJECTED);
        assertThat(second.delay()).isCloseTo(first.delay(), TOLERANCE);
    }

    @Test
    void usersAndChannelsHaveSeparateBuckets() {
        RateLimiter limiter = limiter(1, Duration.ofHours(1), Duration.ZERO);
        String alice = uniqueUser();
        String bob = uniqueUser();

        assertThat(limiter.acquire(alice, "SMS")).isEqualTo(Decision.NOW);
        assertThat(limiter.acquire(alice, "SMS").outcome()).isEqualTo(Outcome.REJECTED);
        assertThat(limiter.acquire(bob, "SMS")).isEqualTo(Decision.NOW);
        assertThat(limiter.acquire(alice, "EMAIL")).isEqualTo(Decision.NOW);
    }

    @Test
    void highPriorityHasItsOwnBucket() {
        RateLimiter limiter = limiter(1, Duration.ofHours(1), Duration.ZERO);
        String user = uniqueUser();

        assertThat(limiter.acquire(user, "SMS", false)).isEqualTo(Decision.NOW);
        assertThat(limiter.acquire(user, "SMS", false).outcome()).isEqualTo(Outcome.REJECTED);
        // The marketing bucket is empty, but the password reset still goes out.
        assertThat(limiter.acquire(user, "SMS", true)).isEqualTo(Decision.NOW);
    }

    @Test
    void channelWithoutABucketIsUnlimited() {
        RateLimiter limiter = new RedisTokenBucketRateLimiter(redis, new WorkerProperties.RateLimit(true,
                Duration.ZERO, Map.of("SMS", new WorkerProperties.Bucket(1, Duration.ofHours(1))), Map.of()));
        String user = uniqueUser();

        for (int i = 0; i < 10; i++) {
            assertThat(limiter.acquire(user, "WEBHOOK")).isEqualTo(Decision.NOW);
        }
    }

    @Test
    void manyConcurrentRequestsForOneUserNeverExceedTheCapacity() throws Exception {
        RateLimiter limiter = limiter(10, Duration.ofHours(1), Duration.ZERO);
        String user = uniqueUser();

        List<Decision> decisions = runConcurrently(50, () -> limiter.acquire(user, "SMS"));

        assertThat(decisions).filteredOn(d -> d.outcome() == Outcome.NOW).hasSize(10);
        assertThat(decisions).filteredOn(d -> d.outcome() == Outcome.REJECTED).hasSize(40);
    }

    @Test
    void twoWorkersShareOneBucketPerUser() throws Exception {
        // Two limiter instances stand in for two worker processes.
        RateLimiter workerA = limiter(10, Duration.ofHours(1), Duration.ZERO);
        RateLimiter workerB = limiter(10, Duration.ofHours(1), Duration.ZERO);
        String user = uniqueUser();
        AtomicInteger calls = new AtomicInteger();

        List<Decision> decisions = runConcurrently(40,
                () -> (calls.incrementAndGet() % 2 == 0 ? workerA : workerB).acquire(user, "SMS"));

        assertThat(decisions).filteredOn(d -> d.outcome() == Outcome.NOW).hasSize(10);
    }

    @Test
    void concurrentReservationsGetDistinctSlotsSpacedByTheRefillPeriod() throws Exception {
        RateLimiter limiter = limiter(1, Duration.ofSeconds(1), Duration.ofHours(1));
        String user = uniqueUser();

        List<Decision> decisions = runConcurrently(20, () -> limiter.acquire(user, "SMS"));

        assertThat(decisions).filteredOn(d -> d.outcome() == Outcome.NOW).hasSize(1);
        List<Long> slots = decisions.stream()
                .filter(d -> d.outcome() == Outcome.LATER)
                .map(d -> Math.round(d.delay().toMillis() / 1000.0))
                .sorted()
                .toList();
        // 19 reservations, one per second: no two notifications share a slot.
        assertThat(slots).containsExactlyElementsOf(
                LongStream.rangeClosed(1, 19).boxed().toList());
    }

    @Test
    void bucketKeysExpireOnceTheyWouldBeFullAgain() {
        RateLimiter limiter = limiter(1, Duration.ofMillis(500), Duration.ofHours(1));
        String user = uniqueUser();

        limiter.acquire(user, "SMS");

        Long ttl = redis.getExpire(RedisTokenBucketRateLimiter.key(user, "SMS", false),
                TimeUnit.MILLISECONDS);
        assertThat(ttl).isBetween(1L, 1_500L);
    }

    @Test
    void letsNotificationsThroughWhenRedisIsDown() {
        LettuceConnectionFactory deadRedis = new LettuceConnectionFactory(
                new RedisStandaloneConfiguration("127.0.0.1", 1));
        deadRedis.afterPropertiesSet();
        deadRedis.start();
        try {
            RateLimiter limiter = new RedisTokenBucketRateLimiter(new StringRedisTemplate(deadRedis),
                    properties(1, Duration.ofHours(1), Duration.ZERO));

            assertThat(limiter.acquire(uniqueUser(), "SMS")).isEqualTo(Decision.NOW);
        } finally {
            deadRedis.destroy();
        }
    }

    private RateLimiter limiter(int capacity, Duration refill, Duration maxWait) {
        return new RedisTokenBucketRateLimiter(redis, properties(capacity, refill, maxWait));
    }

    private static WorkerProperties.RateLimit properties(int capacity, Duration refill, Duration maxWait) {
        WorkerProperties.Bucket bucket = new WorkerProperties.Bucket(capacity, refill);
        return new WorkerProperties.RateLimit(true, maxWait, Map.of("SMS", bucket, "EMAIL", bucket),
                Map.of("SMS", bucket));
    }

    private static <T> List<T> runConcurrently(int threads, Callable<T> task) throws Exception {
        CountDownLatch start = new CountDownLatch(1);
        List<Future<T>> futures = new ArrayList<>();
        try (ExecutorService pool = Executors.newFixedThreadPool(threads)) {
            for (int i = 0; i < threads; i++) {
                futures.add(pool.submit(() -> {
                    start.await();
                    return task.call();
                }));
            }
            start.countDown();
            List<T> results = new ArrayList<>();
            for (Future<T> future : futures) {
                results.add(future.get());
            }
            return results;
        }
    }

    private static String uniqueUser() {
        return "user-" + UUID.randomUUID().toString().substring(0, 8);
    }
}
