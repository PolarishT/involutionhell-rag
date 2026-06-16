package com.involutionhell.backend.rag.infrastructure.ratelimit;

import com.involutionhell.backend.rag.common.ratelimit.RateLimitWindow;
import org.springframework.stereotype.Component;

import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.Deque;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.LongSupplier;

/**
 * 基于 JVM 内存的滚动窗口限流器。
 */
@Component
public class InMemoryRateLimiter {

    private final ConcurrentMap<String, Bucket> buckets = new ConcurrentHashMap<>();
    private final LongSupplier currentTimeMillis;

    public InMemoryRateLimiter() {
        this(System::currentTimeMillis);
    }

    InMemoryRateLimiter(LongSupplier currentTimeMillis) {
        this.currentTimeMillis = Objects.requireNonNull(currentTimeMillis, "currentTimeMillis");
    }

    public RateLimitDecision tryAcquire(String key, RateLimitWindow[] windows) {
        RateLimitWindow[] normalizedWindows = normalizeWindows(windows);
        long now = currentTimeMillis.getAsLong();
        long maxWindowMillis = maxWindowMillis(normalizedWindows);
        Bucket bucket = buckets.computeIfAbsent(key, ignored -> new Bucket());
        synchronized (bucket) {
            bucket.prune(now - maxWindowMillis);

            RateLimitDecision rejection = null;
            for (RateLimitWindow window : normalizedWindows) {
                long windowMillis = toMillis(window);
                int count = bucket.countSince(now - windowMillis);
                if (count >= window.maxRequests()) {
                    long retryAfterSeconds = retryAfterSeconds(bucket, now, windowMillis);
                    RateLimitDecision candidate = RateLimitDecision.reject(window, retryAfterSeconds);
                    rejection = rejection == null || candidate.retryAfterSeconds() > rejection.retryAfterSeconds()
                            ? candidate
                            : rejection;
                }
            }

            if (rejection != null) {
                return rejection;
            }

            bucket.timestamps.addLast(now);
            return RateLimitDecision.allow();
        }
    }

    private RateLimitWindow[] normalizeWindows(RateLimitWindow[] windows) {
        if (windows == null || windows.length == 0) {
            return new RateLimitWindow[0];
        }
        return java.util.Arrays.stream(windows)
                .filter(Objects::nonNull)
                .filter(window -> window.seconds() > 0 && window.maxRequests() > 0)
                .sorted(Comparator.comparingLong(RateLimitWindow::seconds))
                .toArray(RateLimitWindow[]::new);
    }

    private long maxWindowMillis(RateLimitWindow[] windows) {
        long maxSeconds = 0L;
        for (RateLimitWindow window : windows) {
            maxSeconds = Math.max(maxSeconds, window.seconds());
        }
        return Math.max(1L, maxSeconds) * 1_000L;
    }

    private long toMillis(RateLimitWindow window) {
        return window.seconds() * 1_000L;
    }

    private long retryAfterSeconds(Bucket bucket, long now, long windowMillis) {
        Long oldest = bucket.oldestSince(now - windowMillis);
        if (oldest == null) {
            return 1L;
        }
        long retryAfterMillis = oldest + windowMillis - now;
        return Math.max(1L, (retryAfterMillis + 999L) / 1_000L);
    }

    private static final class Bucket {

        private final Deque<Long> timestamps = new ArrayDeque<>();

        private void prune(long minExclusive) {
            while (!timestamps.isEmpty() && timestamps.peekFirst() <= minExclusive) {
                timestamps.removeFirst();
            }
        }

        private int countSince(long minExclusive) {
            int count = 0;
            for (Long timestamp : timestamps) {
                if (timestamp > minExclusive) {
                    count++;
                }
            }
            return count;
        }

        private Long oldestSince(long minExclusive) {
            for (Long timestamp : timestamps) {
                if (timestamp > minExclusive) {
                    return timestamp;
                }
            }
            return null;
        }
    }
}
