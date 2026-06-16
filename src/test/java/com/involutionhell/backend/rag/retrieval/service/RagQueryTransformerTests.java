package com.involutionhell.backend.rag.retrieval.service;

import com.involutionhell.backend.rag.retrieval.observability.RagRetrievalMetrics;
import com.involutionhell.backend.rag.shared.properties.RagProperties;
import com.involutionhell.backend.rag.shared.support.RagOpenAiTokenCounter;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.ai.rag.Query;
import org.springframework.ai.rag.preretrieval.query.transformation.QueryTransformer;
import org.springframework.beans.factory.ObjectProvider;

import static org.assertj.core.api.Assertions.assertThat;

class RagQueryTransformerTests {

    @Test
    void skipsRewriteWhenConfidenceIsHigh() {
        AtomicInteger rewriteCalls = new AtomicInteger();
        RagProperties properties = properties(true, true);
        RagQueryTransformer transformer = transformer(
                properties,
                query -> {
                    rewriteCalls.incrementAndGet();
                    return new Query("rewritten", query.history(), query.context());
                },
                new FixedConfidenceEvaluator(RagQueryRewriteConfidenceResult.available(0.80d, "query is clear")),
                new RagRetrievalMetrics(emptyProvider())
        );

        RagQueryTransformationResult result = transformer.transform("clear query", List.of());

        assertThat(result.retrievalQuestion()).isEqualTo("clear query");
        assertThat(result.queryTransformed()).isFalse();
        assertThat(result.rewriteConfidence()).isEqualTo(0.80d);
        assertThat(result.rewriteDecision()).isEqualTo("skipped_high_confidence");
        assertThat(result.rewriteConfidenceReason()).isEqualTo("query is clear");
        assertThat(rewriteCalls).hasValue(0);
    }

    @Test
    void appliesRewriteWhenConfidenceIsLow() {
        AtomicInteger rewriteCalls = new AtomicInteger();
        RagProperties properties = properties(true, true);
        RagQueryTransformer transformer = transformer(
                properties,
                query -> {
                    rewriteCalls.incrementAndGet();
                    return new Query("rewritten query", query.history(), query.context());
                },
                new FixedConfidenceEvaluator(RagQueryRewriteConfidenceResult.available(0.40d, "query is vague")),
                new RagRetrievalMetrics(emptyProvider())
        );

        RagQueryTransformationResult result = transformer.transform("it", List.of());

        assertThat(result.retrievalQuestion()).isEqualTo("rewritten query");
        assertThat(result.queryTransformed()).isTrue();
        assertThat(result.transformedByModel()).isTrue();
        assertThat(result.rewriteConfidence()).isEqualTo(0.40d);
        assertThat(result.rewriteDecision()).isEqualTo("applied_low_confidence");
        assertThat(result.rewriteConfidenceReason()).isEqualTo("query is vague");
        assertThat(rewriteCalls).hasValue(1);
    }

    @Test
    void skipsRewriteAndRecordsFallbackWhenConfidenceUnavailable() {
        AtomicInteger rewriteCalls = new AtomicInteger();
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        RagRetrievalMetrics metrics = new RagRetrievalMetrics(provider(meterRegistry));
        RagProperties properties = properties(true, true);
        RagQueryTransformer transformer = transformer(
                properties,
                query -> {
                    rewriteCalls.incrementAndGet();
                    return new Query("rewritten", query.history(), query.context());
                },
                new FixedConfidenceEvaluator(RagQueryRewriteConfidenceResult.unavailable("timeout")),
                metrics
        );

        RagQueryTransformationResult result = transformer.transform("ambiguous", List.of());

        assertThat(result.retrievalQuestion()).isEqualTo("ambiguous");
        assertThat(result.rewriteConfidence()).isNull();
        assertThat(result.rewriteDecision()).isEqualTo("skipped_error");
        assertThat(result.rewriteConfidenceReason()).isEqualTo("timeout");
        assertThat(rewriteCalls).hasValue(0);
        Counter counter = meterRegistry.find("rag.retrieval.fallback.count")
                .tag("scope", "query_transform")
                .tag("branch", "rewrite_confidence")
                .tag("reason", "timeout")
                .counter();
        assertThat(counter).isNotNull();
        assertThat(counter.count()).isEqualTo(1.0d);
    }

    @Test
    void skipsRewriteAndRecordsFallbackWhenConfidenceEvaluatorThrows() {
        AtomicInteger rewriteCalls = new AtomicInteger();
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        RagRetrievalMetrics metrics = new RagRetrievalMetrics(provider(meterRegistry));
        RagProperties properties = properties(true, true);
        RagQueryTransformer transformer = transformer(
                properties,
                query -> {
                    rewriteCalls.incrementAndGet();
                    return new Query("rewritten", query.history(), query.context());
                },
                new ThrowingConfidenceEvaluator(),
                metrics
        );

        RagQueryTransformationResult result = transformer.transform("ambiguous", List.of());

        assertThat(result.retrievalQuestion()).isEqualTo("ambiguous");
        assertThat(result.rewriteConfidence()).isNull();
        assertThat(result.rewriteDecision()).isEqualTo("skipped_error");
        assertThat(result.rewriteConfidenceReason()).contains("boom");
        assertThat(rewriteCalls).hasValue(0);
        Counter counter = meterRegistry.find("rag.retrieval.fallback.count")
                .tag("scope", "query_transform")
                .tag("branch", "rewrite_confidence")
                .tag("reason", "error")
                .counter();
        assertThat(counter).isNotNull();
        assertThat(counter.count()).isEqualTo(1.0d);
    }

    @Test
    void usesLegacyRewriteWhenConfidenceGateIsDisabled() {
        AtomicInteger rewriteCalls = new AtomicInteger();
        FixedConfidenceEvaluator evaluator = new FixedConfidenceEvaluator(RagQueryRewriteConfidenceResult.available(0.99d, "ignored"));
        RagProperties properties = properties(true, false);
        RagQueryTransformer transformer = transformer(
                properties,
                query -> {
                    rewriteCalls.incrementAndGet();
                    return new Query("legacy rewritten", query.history(), query.context());
                },
                evaluator,
                new RagRetrievalMetrics(emptyProvider())
        );

        RagQueryTransformationResult result = transformer.transform("clear query", List.of());

        assertThat(result.retrievalQuestion()).isEqualTo("legacy rewritten");
        assertThat(result.rewriteConfidence()).isNull();
        assertThat(result.rewriteDecision()).isEqualTo("applied_low_confidence");
        assertThat(result.rewriteConfidenceReason()).contains("legacy rewrite");
        assertThat(evaluator.calls).isZero();
        assertThat(rewriteCalls).hasValue(1);
    }

    @Test
    void skipsConfidenceAndRewriteWhenRewriteIsDisabled() {
        AtomicInteger rewriteCalls = new AtomicInteger();
        FixedConfidenceEvaluator evaluator = new FixedConfidenceEvaluator(RagQueryRewriteConfidenceResult.available(0.10d, "ignored"));
        RagProperties properties = properties(false, true);
        RagQueryTransformer transformer = transformer(
                properties,
                query -> {
                    rewriteCalls.incrementAndGet();
                    return new Query("rewritten", query.history(), query.context());
                },
                evaluator,
                new RagRetrievalMetrics(emptyProvider())
        );

        RagQueryTransformationResult result = transformer.transform("ambiguous", List.of());

        assertThat(result.retrievalQuestion()).isEqualTo("ambiguous");
        assertThat(result.rewriteDecision()).isEqualTo("skipped_disabled");
        assertThat(evaluator.calls).isZero();
        assertThat(rewriteCalls).hasValue(0);
    }

    @Test
    void queryTransformationDefaultsIncludeRewriteConfidenceGate() {
        RagProperties.QueryTransformation properties = RagProperties.defaults().queryTransformation();

        assertThat(properties.rewriteConfidenceEnabled()).isTrue();
        assertThat(properties.rewriteConfidenceThreshold()).isEqualTo(0.65d);
        assertThat(properties.rewriteConfidenceTimeoutMillis()).isEqualTo(1_000L);
    }

    private RagQueryTransformer transformer(
            RagProperties properties,
            QueryTransformer rewriteTransformer,
            RagQueryRewriteConfidenceEvaluator evaluator,
            RagRetrievalMetrics metrics
    ) {
        return new RagQueryTransformer(
                rewriteTransformer,
                null,
                evaluator,
                properties,
                new RagOpenAiTokenCounter(properties),
                metrics
        );
    }

    private RagProperties properties(boolean rewriteEnabled, boolean rewriteConfidenceEnabled) {
        RagProperties defaults = RagProperties.defaults();
        RagProperties.QueryTransformation transformation = defaults.queryTransformation();
        RagProperties.QueryTransformation queryTransformation = new RagProperties.QueryTransformation(
                transformation.enabled(),
                transformation.useModel(),
                transformation.minConversationTurns(),
                transformation.queryTemplate(),
                transformation.minQuestionLength(),
                transformation.minQuestionTokens(),
                transformation.minHistoryTurns(),
                transformation.timeoutMillis(),
                rewriteEnabled,
                transformation.rewriteUseModel(),
                transformation.rewriteQueryTemplate(),
                transformation.rewriteTargetSearchSystem(),
                rewriteConfidenceEnabled,
                transformation.rewriteConfidenceThreshold(),
                transformation.rewriteConfidenceTimeoutMillis(),
                transformation.rewriteConfidencePromptTemplate()
        );
        return new RagProperties(
                defaults.defaultTopK(),
                defaults.chunkSize(),
                defaults.chunkOverlap(),
                defaults.embeddingModel(),
                defaults.rocketMq(),
                defaults.milvus(),
                defaults.indexing(),
                queryTransformation,
                defaults.queryExpansion(),
                defaults.retrieval(),
                defaults.outbox(),
                defaults.recovery()
        );
    }

    private static <T> ObjectProvider<T> emptyProvider() {
        return provider(null);
    }

    private static <T> ObjectProvider<T> provider(T value) {
        return new ObjectProvider<>() {
            @Override
            public T getObject(Object... args) {
                return value;
            }

            @Override
            public T getObject() {
                return value;
            }
        };
    }

    private static final class FixedConfidenceEvaluator extends RagQueryRewriteConfidenceEvaluator {

        private final RagQueryRewriteConfidenceResult result;
        private int calls;

        private FixedConfidenceEvaluator(RagQueryRewriteConfidenceResult result) {
            this.result = result;
        }

        @Override
        public RagQueryRewriteConfidenceResult evaluate(Query query) {
            calls++;
            return result;
        }
    }

    private static final class ThrowingConfidenceEvaluator extends RagQueryRewriteConfidenceEvaluator {

        @Override
        public RagQueryRewriteConfidenceResult evaluate(Query query) {
            throw new IllegalStateException("boom");
        }
    }
}
