package com.involutionhell.backend.rag.retrieval.service;

import com.involutionhell.backend.rag.shared.properties.RagProperties;
import com.involutionhell.backend.rag.shared.support.RagJsonCodec;
import com.involutionhell.backend.rag.shared.support.RagLogHelper;
import jakarta.annotation.PreDestroy;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.rag.Query;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

/**
 * 使用模型判断原 query 不经 rewrite 也适合检索的置信度。
 */
@Service
public class RagQueryRewriteConfidenceEvaluator {

    private static final Logger log = LoggerFactory.getLogger(RagQueryRewriteConfidenceEvaluator.class);

    private static final String DEFAULT_SYSTEM_PROMPT = """
            你是 RAG 检索 query 质量评估器。
            判断用户当前 query 不经过 rewrite 是否已经适合直接检索知识库。
            只返回 JSON，不要输出 Markdown。
            JSON 格式：{"confidence":0.0到1.0之间的数字,"reason":"20字以内中文原因"}。
            confidence 越高表示原 query 越清晰、独立、可检索；越低表示需要 rewrite。
            """;

    private final ObjectProvider<ChatModel> chatModelProvider;
    private final RagProperties ragProperties;
    private final RagJsonCodec jsonCodec;
    private final ExecutorService executorService;

    protected RagQueryRewriteConfidenceEvaluator() {
        this.chatModelProvider = null;
        this.ragProperties = null;
        this.jsonCodec = null;
        this.executorService = null;
    }

    public RagQueryRewriteConfidenceEvaluator(
            ObjectProvider<ChatModel> chatModelProvider,
            RagProperties ragProperties,
            RagJsonCodec jsonCodec
    ) {
        this.chatModelProvider = chatModelProvider;
        this.ragProperties = ragProperties;
        this.jsonCodec = jsonCodec;
        this.executorService = Executors.newVirtualThreadPerTaskExecutor();
    }

    public RagQueryRewriteConfidenceResult evaluate(Query query) {
        if (chatModelProvider == null || ragProperties == null || jsonCodec == null || executorService == null) {
            return RagQueryRewriteConfidenceResult.unavailable("not_configured");
        }

        ChatModel chatModel = resolveChatModel();
        if (chatModel == null) {
            return RagQueryRewriteConfidenceResult.unavailable("no_chat_model");
        }

        long timeoutMillis = ragProperties.queryTransformation().rewriteConfidenceTimeoutMillis();
        Future<String> future = executorService.submit(() -> callModel(chatModel, query));
        try {
            String content = timeoutMillis > 0
                    ? future.get(timeoutMillis, TimeUnit.MILLISECONDS)
                    : future.get();
            return parse(content);
        } catch (TimeoutException exception) {
            future.cancel(true);
            log.warn("Query rewrite confidence evaluation timed out: queryPreview={}", RagLogHelper.previewQuestion(query.text()));
            return RagQueryRewriteConfidenceResult.unavailable("timeout");
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            future.cancel(true);
            return RagQueryRewriteConfidenceResult.unavailable("interrupted");
        } catch (ExecutionException exception) {
            log.warn(
                    "Query rewrite confidence evaluation failed: queryPreview={}, error={}",
                    RagLogHelper.previewQuestion(query.text()),
                    RagLogHelper.errorSummary(exception.getCause() == null ? exception : exception.getCause())
            );
            return RagQueryRewriteConfidenceResult.unavailable("error");
        } catch (RuntimeException exception) {
            log.warn(
                    "Query rewrite confidence evaluation returned invalid response: queryPreview={}, error={}",
                    RagLogHelper.previewQuestion(query.text()),
                    RagLogHelper.errorSummary(exception)
            );
            return RagQueryRewriteConfidenceResult.unavailable("invalid_response");
        }
    }

    @PreDestroy
    void shutdown() {
        if (executorService != null) {
            executorService.shutdownNow();
        }
    }

    private ChatModel resolveChatModel() {
        try {
            return chatModelProvider.getIfAvailable();
        } catch (Exception exception) {
            log.warn("ChatModel unavailable for query rewrite confidence evaluation: error={}", RagLogHelper.errorSummary(exception));
            return null;
        }
    }

    private String callModel(ChatModel chatModel, Query query) {
        return ChatClient.create(chatModel)
                .prompt()
                .system(systemPrompt())
                .user(userPrompt(query))
                .call()
                .content();
    }

    private String systemPrompt() {
        String template = ragProperties.queryTransformation().rewriteConfidencePromptTemplate();
        return StringUtils.hasText(template) ? template : DEFAULT_SYSTEM_PROMPT;
    }

    private String userPrompt(Query query) {
        return "Query:\n" + (query.text() == null ? "" : query.text().trim());
    }

    private RagQueryRewriteConfidenceResult parse(String content) {
        if (!StringUtils.hasText(content)) {
            throw new IllegalArgumentException("empty_response");
        }

        Map<String, Object> payload = jsonCodec.readMap(extractJson(content));
        Double confidence = asDouble(payload.get("confidence"));
        if (confidence == null) {
            throw new IllegalArgumentException("missing_confidence");
        }
        String reason = payload.get("reason") == null ? null : String.valueOf(payload.get("reason"));
        return RagQueryRewriteConfidenceResult.available(confidence, reason);
    }

    private String extractJson(String content) {
        String trimmed = content.trim();
        if (trimmed.startsWith("```")) {
            trimmed = trimmed.replaceFirst("(?is)^```(?:json)?\\s*", "")
                    .replaceFirst("(?is)\\s*```$", "")
                    .trim();
        }
        int start = trimmed.indexOf('{');
        int end = trimmed.lastIndexOf('}');
        if (start < 0 || end <= start) {
            throw new IllegalArgumentException("json_object_not_found");
        }
        return trimmed.substring(start, end + 1);
    }

    private Double asDouble(Object value) {
        if (value instanceof Number number) {
            return number.doubleValue();
        }
        if (value instanceof String text && StringUtils.hasText(text)) {
            try {
                return Double.parseDouble(text.trim().toLowerCase(Locale.ROOT));
            } catch (NumberFormatException ignored) {
                return null;
            }
        }
        return null;
    }
}
