package com.involutionhell.backend.rag.common.ratelimit;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.ANNOTATION_TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * {@link RateLimit} 的单个滚动时间窗配置。
 *
 * @param seconds 时间窗长度，单位秒
 * @param maxRequests 该时间窗内允许的最大请求次数
 */
@Documented
@Retention(RUNTIME)
@Target(ANNOTATION_TYPE)
public @interface RateLimitWindow {

    long seconds();

    int maxRequests();
}
