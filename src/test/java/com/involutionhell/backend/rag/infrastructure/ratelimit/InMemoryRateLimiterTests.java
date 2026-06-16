package com.involutionhell.backend.rag.infrastructure.ratelimit;

import com.involutionhell.backend.rag.common.ratelimit.RateLimit;
import com.involutionhell.backend.rag.common.ratelimit.RateLimitWindow;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class InMemoryRateLimiterTests {

    @Test
    void rejectsWhenSingleRollingWindowIsExceeded() throws NoSuchMethodException {
        AtomicLong now = new AtomicLong();
        InMemoryRateLimiter limiter = new InMemoryRateLimiter(now::get);
        RateLimitWindow[] windows = windows("twoPerMinute");

        assertThat(limiter.tryAcquire("key", windows).allowed()).isTrue();
        assertThat(limiter.tryAcquire("key", windows).allowed()).isTrue();

        RateLimitDecision rejected = limiter.tryAcquire("key", windows);

        assertThat(rejected.allowed()).isFalse();
        assertThat(rejected.window().seconds()).isEqualTo(60L);
        assertThat(rejected.retryAfterSeconds()).isEqualTo(60L);

        now.addAndGet(60_001L);
        assertThat(limiter.tryAcquire("key", windows).allowed()).isTrue();
    }

    @Test
    void rejectsWhenAnyConfiguredWindowIsExceeded() throws NoSuchMethodException {
        AtomicLong now = new AtomicLong();
        InMemoryRateLimiter limiter = new InMemoryRateLimiter(now::get);
        RateLimitWindow[] windows = windows("twoPerFiveMinutes");

        assertThat(limiter.tryAcquire("key", windows).allowed()).isTrue();
        assertThat(limiter.tryAcquire("key", windows).allowed()).isTrue();

        RateLimitDecision rejected = limiter.tryAcquire("key", windows);

        assertThat(rejected.allowed()).isFalse();
        assertThat(rejected.window().seconds()).isEqualTo(300L);
        assertThat(rejected.retryAfterSeconds()).isEqualTo(300L);
    }

    private RateLimitWindow[] windows(String methodName) throws NoSuchMethodException {
        return WindowDefinitions.class.getDeclaredMethod(methodName)
                .getAnnotation(RateLimit.class)
                .windows();
    }

    private static final class WindowDefinitions {

        @RateLimit(windows = @RateLimitWindow(seconds = 60, maxRequests = 2))
        private static void twoPerMinute() {
        }

        @RateLimit(windows = {
                @RateLimitWindow(seconds = 60, maxRequests = 10),
                @RateLimitWindow(seconds = 300, maxRequests = 2)
        })
        private static void twoPerFiveMinutes() {
        }
    }
}
