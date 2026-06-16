package com.involutionhell.backend.rag.infrastructure.web;

import com.involutionhell.backend.rag.common.api.ApiResponse;
import com.involutionhell.backend.rag.common.ratelimit.RateLimitExceededException;
import com.involutionhell.backend.rag.shared.support.RagLogHelper;
import jakarta.validation.ConstraintViolationException;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.validation.BindException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.bind.support.WebExchangeBindException;
import org.springframework.web.server.ResponseStatusException;

@RestControllerAdvice(basePackages = "com.involutionhell.backend.rag")
@Order(Ordered.HIGHEST_PRECEDENCE)
public class RagExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(RagExceptionHandler.class);

    @ExceptionHandler(RateLimitExceededException.class)
    public ResponseEntity<ApiResponse<Void>> handleRateLimitExceeded(RateLimitExceededException exception) {
        return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                .header(HttpHeaders.RETRY_AFTER, String.valueOf(exception.retryAfterSeconds()))
                .body(ApiResponse.fail(exception.getReason()));
    }

    /**
     * 保留业务层显式声明的 HTTP 语义，例如 404 会话不存在、409 会话归属冲突。
     */
    @ExceptionHandler(ResponseStatusException.class)
    public ResponseEntity<ApiResponse<Void>> handleResponseStatusException(ResponseStatusException exception) {
        HttpStatus status = HttpStatus.valueOf(exception.getStatusCode().value());
        String message = exception.getReason() == null ? status.getReasonPhrase() : exception.getReason();

        return ResponseEntity.status(status)
                .body(ApiResponse.fail(message));
    }

    /**
     * WebFlux 参数校验异常。
     */
    @ExceptionHandler({
            WebExchangeBindException.class,
            BindException.class,
            ConstraintViolationException.class
    })
    public ResponseEntity<ApiResponse<Void>> handleValidation(Exception exception) {
        return ResponseEntity.badRequest()
                .body(ApiResponse.fail(resolveValidationMessage(exception)));
    }

    /**
     * 处理 RAG 业务逻辑中主动抛出的校验异常。
     */
    @ExceptionHandler({IllegalArgumentException.class, IllegalStateException.class})
    public ResponseEntity<ApiResponse<Void>> handleBusinessException(
            Exception exception,
            ServerHttpRequest request
    ) {
        log.warn("RAG 模块业务流程拦截: method={}, path={}, error={}",
                request.getMethod(),
                request.getURI().getPath(),
                RagLogHelper.errorSummary(exception)
        );

        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(ApiResponse.fail("RAG 业务异常: " + exception.getMessage()));
    }

    /**
     * 兜底处理 RAG 模块其他未预期的运行时异常。
     */
    @ExceptionHandler(Exception.class)
    public ResponseEntity<ApiResponse<Void>> handleUnexpectedException(
            Exception exception,
            ServerHttpRequest request
    ) {
        log.error("RAG 模块发生未预期的系统异常: method={}, path={}, error={}",
                request.getMethod(),
                request.getURI().getPath(),
                RagLogHelper.errorSummary(exception),
                exception
        );

        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(ApiResponse.fail("RAG 系统异常，请稍后重试或联系管理员"));
    }

    private String resolveValidationMessage(Exception exception) {
        if (exception instanceof WebExchangeBindException webExchangeBindException) {
            return Optional.ofNullable(webExchangeBindException.getBindingResult().getFieldError())
                    .map(fieldError -> fieldError.getField() + ": " + fieldError.getDefaultMessage())
                    .orElse("请求参数不合法");
        }

        if (exception instanceof BindException bindException) {
            return Optional.ofNullable(bindException.getBindingResult().getFieldError())
                    .map(fieldError -> fieldError.getField() + ": " + fieldError.getDefaultMessage())
                    .orElse("请求参数不合法");
        }

        if (exception instanceof ConstraintViolationException constraintViolationException) {
            return constraintViolationException.getConstraintViolations().stream()
                    .findFirst()
                    .map(violation -> violation.getPropertyPath() + ": " + violation.getMessage())
                    .orElse("请求参数不合法");
        }

        return "请求参数不合法";
    }
}