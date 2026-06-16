package com.involutionhell.backend.rag.common.ratelimit;

import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

/**
 * 方法级限流超限异常。
 */
public class RateLimitExceededException extends ResponseStatusException {

    private final String rateLimitKey;
    private final long windowSeconds;
    private final int maxRequests;
    private final long retryAfterSeconds;

    public RateLimitExceededException(
            String rateLimitKey,
            long windowSeconds,
            int maxRequests,
            long retryAfterSeconds
    ) {
        super(HttpStatus.TOO_MANY_REQUESTS, "请求过于频繁，请稍后再试");
        this.rateLimitKey = rateLimitKey;
        this.windowSeconds = windowSeconds;
        this.maxRequests = maxRequests;
        this.retryAfterSeconds = Math.max(1L, retryAfterSeconds);
    }

    public String rateLimitKey() {
        return rateLimitKey;
    }

    public long windowSeconds() {
        return windowSeconds;
    }

    public int maxRequests() {
        return maxRequests;
    }

    public long retryAfterSeconds() {
        return retryAfterSeconds;
    }
}
