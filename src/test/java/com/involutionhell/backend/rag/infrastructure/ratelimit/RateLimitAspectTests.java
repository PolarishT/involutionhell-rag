package com.involutionhell.backend.rag.infrastructure.ratelimit;

import com.involutionhell.backend.rag.common.ratelimit.RateLimit;
import com.involutionhell.backend.rag.common.ratelimit.RateLimitExceededException;
import com.involutionhell.backend.rag.common.ratelimit.RateLimitWindow;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringJUnitConfig(classes = RateLimitAspectTests.Config.class)
class RateLimitAspectTests {

    @Autowired
    private TestService testService;

    @Test
    void partitionsCountersByResolvedSpelKey() {
        assertThatCode(() -> testService.byUser("alice")).doesNotThrowAnyException();
        assertThatCode(() -> testService.byUser("alice")).doesNotThrowAnyException();

        assertThatThrownBy(() -> testService.byUser("alice"))
                .isInstanceOf(RateLimitExceededException.class);
        assertThatCode(() -> testService.byUser("bob")).doesNotThrowAnyException();
    }

    @Test
    void fallsBackToMethodGlobalWhenSpelEvaluationFails() {
        assertThatCode(() -> testService.badKey("alice")).doesNotThrowAnyException();

        assertThatThrownBy(() -> testService.badKey("bob"))
                .isInstanceOf(RateLimitExceededException.class);
    }

    @Test
    void usesMethodGlobalLimitWhenKeyIsBlank() {
        assertThatCode(() -> testService.global()).doesNotThrowAnyException();

        assertThatThrownBy(() -> testService.global())
                .isInstanceOf(RateLimitExceededException.class);
    }

    @Configuration
    @EnableAspectJAutoProxy(proxyTargetClass = true)
    static class Config {

        @Bean
        InMemoryRateLimiter inMemoryRateLimiter() {
            return new InMemoryRateLimiter();
        }

        @Bean
        RateLimitAspect rateLimitAspect(InMemoryRateLimiter rateLimiter) {
            return new RateLimitAspect(rateLimiter);
        }

        @Bean
        TestService testService() {
            return new TestService();
        }
    }

    static class TestService {

        @RateLimit(key = "#userId", windows = @RateLimitWindow(seconds = 60, maxRequests = 2))
        public String byUser(String userId) {
            return userId;
        }

        @RateLimit(key = "#missing.nope", windows = @RateLimitWindow(seconds = 60, maxRequests = 1))
        public String badKey(String userId) {
            return userId;
        }

        @RateLimit(windows = @RateLimitWindow(seconds = 60, maxRequests = 1))
        public String global() {
            return "ok";
        }
    }
}
