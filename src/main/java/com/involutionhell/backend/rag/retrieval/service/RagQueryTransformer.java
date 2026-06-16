package com.involutionhell.backend.rag.retrieval.service;

import com.involutionhell.backend.rag.retrieval.api.RagConversationMessage;
import com.involutionhell.backend.rag.retrieval.observability.RagRetrievalMetrics;
import com.involutionhell.backend.rag.shared.properties.RagProperties;
import com.involutionhell.backend.rag.shared.support.RagLogHelper;
import com.involutionhell.backend.rag.shared.support.RagOpenAiTokenCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.chat.messages.AssistantMessage;
import org.springframework.ai.chat.messages.Message;
import org.springframework.ai.chat.messages.SystemMessage;
import org.springframework.ai.chat.messages.UserMessage;
import org.springframework.ai.rag.Query;
import org.springframework.ai.rag.preretrieval.query.transformation.CompressionQueryTransformer;
import org.springframework.ai.rag.preretrieval.query.transformation.QueryTransformer;
import org.springframework.ai.rag.preretrieval.query.transformation.RewriteQueryTransformer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * 对接 Spring AI 官方的 CompressionQueryTransformer / RewriteQueryTransformer，
 * 先按阈值决定是否压缩，再在 query expand 前完成 rewrite。
 */
@Service
public class RagQueryTransformer {

    private static final Logger log = LoggerFactory.getLogger(RagQueryTransformer.class);
    private static final String REWRITE_APPLIED_LOW_CONFIDENCE = "applied_low_confidence";
    private static final String REWRITE_SKIPPED_HIGH_CONFIDENCE = "skipped_high_confidence";
    private static final String REWRITE_SKIPPED_DISABLED = "skipped_disabled";
    private static final String REWRITE_SKIPPED_UNAVAILABLE = "skipped_unavailable";
    private static final String REWRITE_SKIPPED_ERROR = "skipped_error";
    private static final String REWRITE_SKIPPED_EMPTY_RESULT = "skipped_empty_result";

    private final RagProperties ragProperties;
    private final QueryTransformer compressionQueryTransformer;
    private final QueryTransformer rewriteQueryTransformer;
    private final RagQueryRewriteConfidenceEvaluator rewriteConfidenceEvaluator;
    private final RagOpenAiTokenCounter tokenCounter;
    private final RagRetrievalMetrics retrievalMetrics;

    @Autowired
    public RagQueryTransformer(
            ObjectProvider<RewriteQueryTransformer> rewriteQueryTransformerProvider,
            ObjectProvider<CompressionQueryTransformer> compressionQueryTransformerProvider,
            RagQueryRewriteConfidenceEvaluator rewriteConfidenceEvaluator,
            RagProperties ragProperties,
            RagOpenAiTokenCounter tokenCounter,
            RagRetrievalMetrics retrievalMetrics
    ) {
        this(
                resolveRewriteQueryTransformer(rewriteQueryTransformerProvider),
                resolveCompressionQueryTransformer(compressionQueryTransformerProvider),
                rewriteConfidenceEvaluator,
                ragProperties,
                tokenCounter,
                retrievalMetrics
        );
    }

    RagQueryTransformer(
            QueryTransformer rewriteQueryTransformer,
            QueryTransformer compressionQueryTransformer,
            RagQueryRewriteConfidenceEvaluator rewriteConfidenceEvaluator,
            RagProperties ragProperties,
            RagOpenAiTokenCounter tokenCounter,
            RagRetrievalMetrics retrievalMetrics
    ) {
        this.ragProperties = ragProperties;
        this.tokenCounter = tokenCounter;
        this.retrievalMetrics = retrievalMetrics;
        this.rewriteQueryTransformer = rewriteQueryTransformer;
        this.compressionQueryTransformer = compressionQueryTransformer;
        this.rewriteConfidenceEvaluator = rewriteConfidenceEvaluator;
    }

    private static RewriteQueryTransformer resolveRewriteQueryTransformer(
            ObjectProvider<RewriteQueryTransformer> rewriteQueryTransformerProvider
    ) {
        try {
            return rewriteQueryTransformerProvider.getIfAvailable();
        } catch (Exception exception) {
            log.warn("RewriteQueryTransformer Bean unavailable, rewrite disabled for this request: error={}", RagLogHelper.errorSummary(exception));
            return null;
        }
    }

    private static CompressionQueryTransformer resolveCompressionQueryTransformer(
            ObjectProvider<CompressionQueryTransformer> compressionQueryTransformerProvider
    ) {
        try {
            return compressionQueryTransformerProvider.getIfAvailable();
        } catch (Exception exception) {
            log.warn("CompressionQueryTransformer Bean unavailable, compression disabled for this request: error={}", RagLogHelper.errorSummary(exception));
            return null;
        }
    }

    public RagQueryTransformationResult transform(String question, List<RagConversationMessage> history) {
        String normalizedQuestion = question == null ? "" : question.trim();
        List<Message> springAiHistory = toSpringAiHistory(history);
        int previousUserTurns = countPreviousUserTurns(history);
        int conversationTurns = normalizedQuestion.isEmpty() ? previousUserTurns : previousUserTurns + 1;
        if (normalizedQuestion.isEmpty()) {
            return new RagQueryTransformationResult(normalizedQuestion, normalizedQuestion, false, false, conversationTurns);
        }

        Query workingQuery = new Query(normalizedQuestion, springAiHistory, Map.of());
        RewriteOutcome rewriteOutcome = RewriteOutcome.skipped(workingQuery, REWRITE_SKIPPED_DISABLED, null, null);
        boolean modelUsed = false;
        boolean compressionApplied = false;
        boolean rewriteApplied = false;

        Query compressedQuery = applyCompression(workingQuery, previousUserTurns, conversationTurns);
        if (compressedQuery != workingQuery) {
            workingQuery = compressedQuery;
            modelUsed = true;
            compressionApplied = true;
        }

        rewriteOutcome = applyRewrite(workingQuery);
        if (rewriteOutcome.query() != workingQuery) {
            workingQuery = rewriteOutcome.query();
            modelUsed = true;
            rewriteApplied = true;
        }

        String retrievalQuestion = workingQuery.text() == null ? "" : workingQuery.text().trim();
        if (retrievalQuestion.isEmpty()) {
            log.warn("Query transformation returned empty retrieval question, falling back to original query.");
            retrievalMetrics.recordFallback("query_transform", "transform", "empty_result");
            return new RagQueryTransformationResult(
                    normalizedQuestion,
                    normalizedQuestion,
                    false,
                    false,
                    conversationTurns,
                    rewriteOutcome.confidence(),
                    rewriteOutcome.decision(),
                    rewriteOutcome.reason()
            );
        }

        boolean transformed = !retrievalQuestion.equals(normalizedQuestion);
        log.debug(
                "Query transformation completed: conversationTurns={}, compressionApplied={}, rewriteApplied={}, transformed={}, originalPreview={}, retrievalPreview={}",
                conversationTurns,
                compressionApplied,
                rewriteApplied,
                transformed,
                RagLogHelper.previewQuestion(normalizedQuestion),
                RagLogHelper.previewQuestion(retrievalQuestion)
        );
        return new RagQueryTransformationResult(
                normalizedQuestion,
                retrievalQuestion,
                transformed,
                modelUsed,
                conversationTurns,
                rewriteOutcome.confidence(),
                rewriteOutcome.decision(),
                rewriteOutcome.reason()
        );
    }

    private Query applyCompression(Query query, int previousUserTurns, int conversationTurns) {
        if (!shouldApplyCompression(query.text(), previousUserTurns, conversationTurns)) {
            return query;
        }

        try {
            Query compressedQuery = compressionQueryTransformer.transform(query);
            String retrievalQuestion = compressedQuery.text() == null ? "" : compressedQuery.text().trim();
            if (retrievalQuestion.isEmpty()) {
                log.warn("CompressionQueryTransformer returned empty result, falling back to pre-compression query.");
                retrievalMetrics.recordFallback("query_transform", "compression", "empty_result");
                return query;
            }
            return new Query(retrievalQuestion, query.history(), query.context());
        } catch (Exception exception) {
            log.warn("CompressionQueryTransformer failed, falling back to pre-compression query: error={}", RagLogHelper.errorSummary(exception));
            retrievalMetrics.recordFallback("query_transform", "compression", "error");
            return query;
        }
    }

    private RewriteOutcome applyRewrite(Query query) {
        RewriteReadiness readiness = rewriteReadiness(query.text());
        if (!readiness.ready()) {
            return RewriteOutcome.skipped(query, readiness.decision(), null, readiness.reason());
        }

        RagQueryRewriteConfidenceResult confidence = null;
        if (ragProperties.queryTransformation().rewriteConfidenceEnabled()) {
            try {
                confidence = rewriteConfidenceEvaluator.evaluate(query);
            } catch (Exception exception) {
                log.warn(
                        "Query rewrite confidence evaluator failed, skipping rewrite: queryPreview={}, error={}",
                        RagLogHelper.previewQuestion(query.text()),
                        RagLogHelper.errorSummary(exception)
                );
                retrievalMetrics.recordFallback("query_transform", "rewrite_confidence", "error");
                return RewriteOutcome.skipped(query, REWRITE_SKIPPED_ERROR, null, RagLogHelper.errorSummary(exception));
            }
            if (confidence == null) {
                retrievalMetrics.recordFallback("query_transform", "rewrite_confidence", "null_response");
                return RewriteOutcome.skipped(query, REWRITE_SKIPPED_ERROR, null, "null_response");
            }
            if (!confidence.available()) {
                String fallbackReason = fallbackReason(confidence.fallbackReason());
                String decision = "no_chat_model".equals(fallbackReason)
                        || "not_configured".equals(fallbackReason)
                        ? REWRITE_SKIPPED_UNAVAILABLE
                        : REWRITE_SKIPPED_ERROR;
                retrievalMetrics.recordFallback("query_transform", "rewrite_confidence", fallbackReason);
                return RewriteOutcome.skipped(query, decision, null, confidence.reason() == null ? fallbackReason : confidence.reason());
            }

            if (confidence.confidence() >= ragProperties.queryTransformation().rewriteConfidenceThreshold()) {
                return RewriteOutcome.skipped(
                        query,
                        REWRITE_SKIPPED_HIGH_CONFIDENCE,
                        confidence.confidence(),
                        confidence.reason()
                );
            }
        }

        try {
            Query rewrittenQuery = rewriteQueryTransformer.transform(query);
            String retrievalQuestion = rewrittenQuery.text() == null ? "" : rewrittenQuery.text().trim();
            if (retrievalQuestion.isEmpty()) {
                log.warn("RewriteQueryTransformer returned empty result, falling back to pre-rewrite query.");
                retrievalMetrics.recordFallback("query_transform", "rewrite", "empty_result");
                return RewriteOutcome.skipped(
                        query,
                        REWRITE_SKIPPED_EMPTY_RESULT,
                        confidence == null ? null : confidence.confidence(),
                        confidence == null ? "rewrite returned empty result" : confidence.reason()
                );
            }
            return new RewriteOutcome(
                    new Query(retrievalQuestion, query.history(), query.context()),
                    confidence == null ? null : confidence.confidence(),
                    REWRITE_APPLIED_LOW_CONFIDENCE,
                    confidence == null ? "rewrite confidence gate disabled; legacy rewrite applied" : confidence.reason()
            );
        } catch (Exception exception) {
            log.warn("RewriteQueryTransformer failed, falling back to pre-rewrite query: error={}", RagLogHelper.errorSummary(exception));
            retrievalMetrics.recordFallback("query_transform", "rewrite", "error");
            return RewriteOutcome.skipped(
                    query,
                    REWRITE_SKIPPED_ERROR,
                    confidence == null ? null : confidence.confidence(),
                    RagLogHelper.errorSummary(exception)
            );
        }
    }

    private String fallbackReason(String reason) {
        return reason == null || reason.isBlank() ? "unavailable" : reason;
    }

    private boolean shouldApplyCompression(String queryText, int previousUserTurns, int conversationTurns) {
        if (!ragProperties.queryTransformation().enabled()
                || !ragProperties.queryTransformation().useModel()
                || this.compressionQueryTransformer == null
                || queryText == null
                || queryText.isBlank()
                || conversationTurns < ragProperties.queryTransformation().minConversationTurns()) {
            return false;
        }

        boolean hasAdditionalThreshold = ragProperties.queryTransformation().minQuestionLength() > 0
                || ragProperties.queryTransformation().minQuestionTokens() > 0
                || ragProperties.queryTransformation().minHistoryTurns() > 0;
        if (!hasAdditionalThreshold) {
            return true;
        }

        int questionLength = queryText.trim().length();
        int questionTokens = approximateTokenCount(queryText);
        return (ragProperties.queryTransformation().minQuestionLength() > 0
                && questionLength >= ragProperties.queryTransformation().minQuestionLength())
                || (ragProperties.queryTransformation().minQuestionTokens() > 0
                && questionTokens >= ragProperties.queryTransformation().minQuestionTokens())
                || (ragProperties.queryTransformation().minHistoryTurns() > 0
                && previousUserTurns >= ragProperties.queryTransformation().minHistoryTurns());
    }

    private RewriteReadiness rewriteReadiness(String queryText) {
        if (!ragProperties.queryTransformation().rewriteEnabled()
                || !ragProperties.queryTransformation().rewriteUseModel()
                || queryText == null
                || queryText.isBlank()) {
            return new RewriteReadiness(false, REWRITE_SKIPPED_DISABLED, "rewrite disabled");
        }
        if (this.rewriteQueryTransformer == null
                || (ragProperties.queryTransformation().rewriteConfidenceEnabled()
                && this.rewriteConfidenceEvaluator == null)) {
            return new RewriteReadiness(false, REWRITE_SKIPPED_UNAVAILABLE, "rewrite unavailable");
        }
        return new RewriteReadiness(true, null, null);
    }

    private int countPreviousUserTurns(List<RagConversationMessage> history) {
        return (int) (history == null ? 0 : history.stream()
                .filter(Objects::nonNull)
                .filter(message -> isUserRole(message.role()))
                .filter(message -> message.content() != null && !message.content().trim().isEmpty())
                .count());
    }

    private List<Message> toSpringAiHistory(List<RagConversationMessage> history) {
        if (history == null || history.isEmpty()) {
            return List.of();
        }

        return history.stream()
                .filter(Objects::nonNull)
                .map(this::toSpringAiMessage)
                .filter(Objects::nonNull)
                .toList();
    }

    private Message toSpringAiMessage(RagConversationMessage message) {
        String content = message.content() == null ? "" : message.content().trim();
        if (content.isEmpty()) {
            return null;
        }

        String role = message.role() == null ? "" : message.role().trim().toLowerCase(Locale.ROOT);
        return switch (role) {
            case "user" -> new UserMessage(content);
            case "assistant" -> new AssistantMessage(content);
            case "system" -> new SystemMessage(content);
            default -> {
                log.debug("忽略不支持的对话角色。role={}", message.role());
                yield null;
            }
        };
    }

    private boolean isUserRole(String role) {
        return role != null && "user".equalsIgnoreCase(role.trim());
    }

    private int approximateTokenCount(String text) {
        return tokenCounter.count(text);
    }

    private record RewriteOutcome(
            Query query,
            Double confidence,
            String decision,
            String reason
    ) {
        private static RewriteOutcome skipped(Query query, String decision, Double confidence, String reason) {
            return new RewriteOutcome(query, confidence, decision, reason);
        }
    }

    private record RewriteReadiness(
            boolean ready,
            String decision,
            String reason
    ) {
    }
}
