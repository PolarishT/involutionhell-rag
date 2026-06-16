package com.involutionhell.backend.rag.infrastructure.ratelimit;

import com.involutionhell.backend.rag.common.ratelimit.RateLimitWindow;

record RateLimitDecision(
        boolean allowed,
        RateLimitWindow window,
        long retryAfterSeconds
) {

    static RateLimitDecision allow() {
        return new RateLimitDecision(true, null, 0L);
    }

    static RateLimitDecision reject(RateLimitWindow window, long retryAfterSeconds) {
        return new RateLimitDecision(false, window, retryAfterSeconds);
    }
}
