package com.involutionhell.backend.rag.infrastructure.ratelimit;

import com.involutionhell.backend.rag.common.ratelimit.RateLimit;
import com.involutionhell.backend.rag.common.ratelimit.RateLimitExceededException;
import com.involutionhell.backend.rag.common.ratelimit.RateLimitWindow;
import com.involutionhell.backend.rag.shared.support.RagLogHelper;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.support.AopUtils;
import org.springframework.core.DefaultParameterNameDiscoverer;
import org.springframework.core.ParameterNameDiscoverer;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 执行 {@link RateLimit} 的 Spring AOP 切面。
 */
@Aspect
@Component
public class RateLimitAspect {

    private static final Logger log = LoggerFactory.getLogger(RateLimitAspect.class);
    private static final String GLOBAL_KEY = "global";

    private final InMemoryRateLimiter rateLimiter;
    private final SpelExpressionParser expressionParser = new SpelExpressionParser();
    private final ParameterNameDiscoverer parameterNameDiscoverer = new DefaultParameterNameDiscoverer();
    private final ConcurrentMap<String, Expression> expressionCache = new ConcurrentHashMap<>();

    public RateLimitAspect(InMemoryRateLimiter rateLimiter) {
        this.rateLimiter = rateLimiter;
    }

    @Around("@annotation(com.involutionhell.backend.rag.common.ratelimit.RateLimit)")
    public Object around(ProceedingJoinPoint joinPoint) throws Throwable {
        Method method = resolveMethod(joinPoint);
        RateLimit rateLimit = AnnotatedElementUtils.findMergedAnnotation(method, RateLimit.class);
        if (rateLimit == null) {
            return joinPoint.proceed();
        }

        String methodKey = methodKey(method);
        String dimensionKey = resolveDimensionKey(rateLimit.key(), method, joinPoint.getArgs());
        String rateLimitKey = methodKey + "::" + dimensionKey;
        RateLimitDecision decision = rateLimiter.tryAcquire(rateLimitKey, rateLimit.windows());
        if (!decision.allowed()) {
            RateLimitWindow window = Objects.requireNonNull(decision.window(), "window");
            throw new RateLimitExceededException(
                    rateLimitKey,
                    window.seconds(),
                    window.maxRequests(),
                    decision.retryAfterSeconds()
            );
        }

        return joinPoint.proceed();
    }

    private Method resolveMethod(ProceedingJoinPoint joinPoint) {
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        Method method = signature.getMethod();
        Object target = joinPoint.getTarget();
        if (target == null) {
            return method;
        }
        return AopUtils.getMostSpecificMethod(method, target.getClass());
    }

    private String resolveDimensionKey(String keyExpression, Method method, Object[] args) {
        if (!StringUtils.hasText(keyExpression)) {
            return GLOBAL_KEY;
        }
        try {
            Object value = expressionCache.computeIfAbsent(keyExpression, expressionParser::parseExpression)
                    .getValue(evaluationContext(method, args));
            if (value == null) {
                return GLOBAL_KEY;
            }
            String key = String.valueOf(value).trim();
            return key.isEmpty() ? GLOBAL_KEY : key;
        } catch (Exception exception) {
            log.debug(
                    "RateLimit SpEL evaluation failed, falling back to method global limit: method={}, key={}, error={}",
                    methodKey(method),
                    keyExpression,
                    RagLogHelper.errorSummary(exception)
            );
            return GLOBAL_KEY;
        }
    }

    private StandardEvaluationContext evaluationContext(Method method, Object[] args) {
        StandardEvaluationContext context = new StandardEvaluationContext();
        Object[] safeArgs = args == null ? new Object[0] : args;
        String[] parameterNames = parameterNameDiscoverer.getParameterNames(method);
        for (int index = 0; index < safeArgs.length; index++) {
            context.setVariable("p" + index, safeArgs[index]);
            context.setVariable("a" + index, safeArgs[index]);
            context.setVariable("arg" + index, safeArgs[index]);
            if (parameterNames != null && index < parameterNames.length && StringUtils.hasText(parameterNames[index])) {
                context.setVariable(parameterNames[index], safeArgs[index]);
            }
        }
        return context;
    }

    private String methodKey(Method method) {
        String parameterTypes = Arrays.stream(method.getParameterTypes())
                .map(Class::getName)
                .reduce((left, right) -> left + "," + right)
                .orElse("");
        return method.getDeclaringClass().getName() + "#" + method.getName() + "(" + parameterTypes + ")";
    }
}
