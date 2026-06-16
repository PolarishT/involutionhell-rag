package com.involutionhell.backend.rag.common.ratelimit;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * 方法级注解式限流。标注在 Spring Bean（典型为 {@code @RestController} 的接口方法）上，
 * 由 {@code RateLimitAspect} 在方法执行前做多时间窗计数校验，超限抛
 * {@link RateLimitExceededException}（HTTP 429）。
 *
 * <p><b>默认窗口</b>（不显式声明 {@link #windows()} 时生效）：
 * <ul>
 *   <li>1 分钟内最多 6 次</li>
 *   <li>5 分钟内最多 16 次</li>
 *   <li>10 分钟内最多 25 次</li>
 * </ul>
 * 三个窗口为「与」关系：任一窗口超限即拒绝。
 *
 * <p><b>限流维度</b>由 {@link #key()} 决定，支持 SpEL：
 * <pre>{@code
 * // 按 userId 维度限流（每个 userId 独立计数）
 * @RateLimit(key = "#userId")
 * @PostMapping("/ask")
 * public ... ask(@RequestParam String userId, ...) { ... }
 *
 * // 按入参对象字段限流
 * @RateLimit(key = "#request.userId")
 * public ... ask(@RequestBody AskRequest request) { ... }
 *
 * // 复合维度
 * @RateLimit(key = "#userId + ':' + #conversationId")
 *
 * // 不写 key：对该方法做全局限流（所有调用方共享一份计数）
 * @RateLimit
 * }</pre>
 *
 * <p>SpEL 解析失败或求值为 {@code null} 时，自动退化为「方法级全局限流」，不会阻断请求链路。
 */
@Documented
@Retention(RUNTIME)
@Target(METHOD)
public @interface RateLimit {

    String key() default "";

    RateLimitWindow[] windows() default {
            @RateLimitWindow(seconds = 60, maxRequests = 6),
            @RateLimitWindow(seconds = 300, maxRequests = 16),
            @RateLimitWindow(seconds = 600, maxRequests = 25)
    };
}
