package com.involutionhell.backend.rag.retrieval.harness;

import com.involutionhell.backend.rag.indexing.api.IndexingChunkQueryFacade;
import com.involutionhell.backend.rag.indexing.api.RagChunkSearchView;
import com.involutionhell.backend.rag.retrieval.model.RagRetrievedChunk;
import com.involutionhell.backend.rag.retrieval.observability.RagRetrievalMetrics;
import com.involutionhell.backend.rag.retrieval.service.RagDocumentJoiner;
import com.involutionhell.backend.rag.retrieval.service.RagQueryExpander;
import com.involutionhell.backend.rag.retrieval.service.RagQueryExpansionResult;
import com.involutionhell.backend.rag.retrieval.service.RagQueryTransformationResult;
import com.involutionhell.backend.rag.retrieval.service.RagQueryTransformer;
import com.involutionhell.backend.rag.retrieval.service.RagRetrievalBudget;
import com.involutionhell.backend.rag.retrieval.service.RagRetrievalBudgetPlanner;
import com.involutionhell.backend.rag.retrieval.service.RagRetrievalRequest;
import com.involutionhell.backend.rag.retrieval.service.RagRetriever;
import com.involutionhell.backend.rag.retrieval.support.RagRequestFeedbacks;
import com.involutionhell.backend.rag.shared.metadata.RagChunkMetadataHelper;
import com.involutionhell.backend.rag.shared.metadata.RagChunkMetadataView;
import com.involutionhell.backend.rag.shared.properties.RagProperties;
import com.involutionhell.backend.rag.shared.support.RagLogFields;
import com.involutionhell.backend.rag.shared.support.RagLogHelper;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * 默认 query harness：承载 query 侧的阶段编排、降级、并发和观测。
 */
@Service
public class DefaultRagQueryHarness implements RagQueryHarness {

    private static final Logger log = LoggerFactory.getLogger(DefaultRagQueryHarness.class);

    private final IndexingChunkQueryFacade indexingChunkQueryFacade;
    private final RagRetriever ragRetriever;
    private final RagQueryTransformer ragQueryTransformer;
    private final RagQueryExpander ragQueryExpander;
    private final RagDocumentJoiner ragDocumentJoiner;
    private final RagRetrievalBudgetPlanner retrievalBudgetPlanner;
    private final RagProperties ragProperties;
    private final RagChunkMetadataHelper metadataHelper;
    private final RagRetrievalMetrics retrievalMetrics;
    private final Scheduler ragBlockingScheduler;

    @Autowired
    public DefaultRagQueryHarness(
            IndexingChunkQueryFacade indexingChunkQueryFacade,
            RagRetriever ragRetriever,
            RagQueryTransformer ragQueryTransformer,
            RagQueryExpander ragQueryExpander,
            RagDocumentJoiner ragDocumentJoiner,
            RagRetrievalBudgetPlanner retrievalBudgetPlanner,
            RagProperties ragProperties,
            RagChunkMetadataHelper metadataHelper,
            RagRetrievalMetrics retrievalMetrics,
            @Qualifier("ragBlockingScheduler") ObjectProvider<Scheduler> ragBlockingSchedulerProvider
    ) {
        this(
                indexingChunkQueryFacade,
                ragRetriever,
                ragQueryTransformer,
                ragQueryExpander,
                ragDocumentJoiner,
                retrievalBudgetPlanner,
                ragProperties,
                metadataHelper,
                retrievalMetrics,
                ragBlockingSchedulerProvider.getIfAvailable(Schedulers::boundedElastic)
        );
    }

    public DefaultRagQueryHarness(
            IndexingChunkQueryFacade indexingChunkQueryFacade,
            RagRetriever ragRetriever,
            RagQueryTransformer ragQueryTransformer,
            RagQueryExpander ragQueryExpander,
            RagDocumentJoiner ragDocumentJoiner,
            RagRetrievalBudgetPlanner retrievalBudgetPlanner,
            RagProperties ragProperties,
            RagChunkMetadataHelper metadataHelper,
            RagRetrievalMetrics retrievalMetrics,
            Scheduler ragBlockingScheduler
    ) {
        this.indexingChunkQueryFacade = indexingChunkQueryFacade;
        this.ragRetriever = ragRetriever;
        this.ragQueryTransformer = ragQueryTransformer;
        this.ragQueryExpander = ragQueryExpander;
        this.ragDocumentJoiner = ragDocumentJoiner;
        this.retrievalBudgetPlanner = retrievalBudgetPlanner;
        this.ragProperties = ragProperties;
        this.metadataHelper = metadataHelper;
        this.retrievalMetrics = retrievalMetrics;
        this.ragBlockingScheduler = ragBlockingScheduler;
    }

    @Override
    public Flux<RagQueryHarnessEvent> execute(RagQueryHarnessRequest request) {
        return transform(request)
                .flatMapMany(transformedState -> Flux.concat(
                        Flux.just(RagQueryHarnessEvent.transformed(transformedState.transformedQuery())),
                        expand(transformedState)
                                .flatMapMany(expandedState -> Flux.concat(
                                        Flux.just(RagQueryHarnessEvent.expanded(
                                                expandedState.transformed().transformedQuery(),
                                                expandedState.expandedQuery()
                                        )),
                                        retrieveContexts(expandedState)
                                                .map(contexts -> RagQueryHarnessEvent.contexts(
                                                        expandedState.transformed().transformedQuery(),
                                                        expandedState.expandedQuery(),
                                                        contexts
                                                ))
                                ))
                ));
    }

    private Mono<TransformedState> transform(RagQueryHarnessRequest request) {
        // Spring AI transformer 当前是阻塞调用；放到专用 scheduler，避免占用 WebFlux 请求线程。
        Mono<RagQueryTransformationResult> transform = Mono.fromCallable(() -> retrievalMetrics.recordStage(
                        "query_transform",
                        request.hasFilter(),
                        () -> ragQueryTransformer.transform(request.askRequest().question(), request.askRequest().history())
                ))
                .subscribeOn(ragBlockingScheduler);
        long timeoutMillis = ragProperties.queryTransformation().timeoutMillis();
        if (timeoutMillis > 0) {
            transform = transform.timeout(Duration.ofMillis(timeoutMillis));
        }
        return transform
                .onErrorResume(TimeoutException.class, exception -> {
                    retrievalMetrics.recordFallback("query_transform", "transform", "timeout");
                    RagRequestFeedbacks.recordTimeout(
                            request.feedbacks(),
                            "query_transform",
                            "查询改写超时，已使用原始问题继续检索。"
                    );
                    return Mono.just(new RagQueryTransformationResult(
                            request.askRequest().question(),
                            request.askRequest().question(),
                            false,
                            false,
                            request.askRequest().history() == null ? 1 : request.askRequest().history().size() + 1
                    ));
                })
                .map(result -> new TransformedState(request, result));
    }

    private Mono<ExpandedState> expand(TransformedState state) {
        // Query expansion 可能触发模型调用，同样隔离到阻塞 scheduler，并用配置控制阶段预算。
        Mono<RagQueryExpansionResult> expand = Mono.fromCallable(() -> retrievalMetrics.recordStage(
                        "query_expand",
                        state.initial().hasFilter(),
                        () -> {
                            RagQueryExpansionResult result = ragQueryExpander.expand(
                                    state.transformedQuery().retrievalQuestion()
                            );
                            log.atInfo()
                                    .addKeyValue(RagLogFields.RAG_QUERY_EXPANDED, result)
                                    .log("RAG ask expanded");
                            return result;
                        }
                ))
                .subscribeOn(ragBlockingScheduler);
        long timeoutMillis = ragProperties.queryExpansion().timeoutMillis();
        if (timeoutMillis > 0) {
            expand = expand.timeout(Duration.ofMillis(timeoutMillis));
        }
        return expand
                .onErrorResume(TimeoutException.class, exception -> {
                    retrievalMetrics.recordFallback("query_expand", "multi_query", "timeout");
                    RagRequestFeedbacks.recordTimeout(
                            state.initial().feedbacks(),
                            "query_expand",
                            "查询扩展超时，已使用单查询继续检索。"
                    );
                    String retrievalQuestion = state.transformedQuery().retrievalQuestion();
                    return Mono.just(new RagQueryExpansionResult(
                            retrievalQuestion,
                            List.of(retrievalQuestion),
                            false,
                            false
                    ));
                })
                .map(result -> {
                    retrievalMetrics.recordExpandedQueryCount(result.retrievalQueries().size());
                    RagRetrievalBudget budget = retrievalBudgetPlanner.plan(
                            state.initial().askRequest().question(),
                            result.retrievalQueries(),
                            state.initial().filter(),
                            state.initial().topK(),
                            false
                    );
                    logPreprocessed(state, result, budget);
                    return new ExpandedState(state, result, budget);
                });
    }

    private Mono<List<RagRetrievedChunk>> retrieveContexts(ExpandedState state) {
        // 多 query 扇出是本链路主要延迟来源；并发度由配置限制，防止一次请求打满 JDBC/Milvus 资源。
        RagRetrievalBudget initialBudget = state.budget();
        return retrieveWithBudget(state, initialBudget)
                .flatMap(results -> join(state, results, initialBudget.answerTopK())
                        .flatMap(joined -> maybeProgressiveWiden(state, results, joined, initialBudget)))
                .flatMap(joined -> expandNeighborWindow(state, joined))
                .map(contexts -> {
                    if (contexts == null || contexts.isEmpty()) {
                        retrievalMetrics.recordZeroHit("ask");
                    }
                    return contexts == null ? List.<RagRetrievedChunk>of() : contexts;
                });
    }

    private Mono<List<List<RagRetrievedChunk>>> retrieveWithBudget(ExpandedState state, RagRetrievalBudget budget) {
        return Flux.fromIterable(state.expandedQuery().retrievalQueries())
                .flatMap(query -> retrieveOneQuery(query, state, budget), ragProperties.retrieval().queryConcurrency())
                .collectList();
    }

    private Mono<List<RagRetrievedChunk>> retrieveOneQuery(String query, ExpandedState state, RagRetrievalBudget budget) {
        // Retriever 内部仍是同步 API，外层用 Mono 包装以获得 query 级超时和分支级降级。
        Mono<List<RagRetrievedChunk>> retrieve = Mono.fromCallable(() -> ragRetriever.search(new RagRetrievalRequest(
                        query,
                        state.transformed().initial().filter(),
                        budget,
                        state.transformed().initial().feedbacks()
                )))
                .subscribeOn(ragBlockingScheduler);
        long timeoutMillis = ragProperties.retrieval().queryTimeoutMillis();
        if (timeoutMillis > 0) {
            retrieve = retrieve.timeout(Duration.ofMillis(timeoutMillis));
        }
        return retrieve.onErrorResume(exception -> {
            retrievalMetrics.recordFallback("retrieve", "query", isTimeout(exception) ? "timeout" : "error");
            String message = isTimeout(exception)
                    ? "单路检索超时，已跳过该查询分支。"
                    : "单路检索失败，已跳过该查询分支。";
            RagRequestFeedbacks.record(
                    state.transformed().initial().feedbacks(),
                    "retrieve",
                    isTimeout(exception) ? "timeout" : "error",
                    message
            );
            log.warn(
                    "RAG query branch failed and will be skipped: queryPreview={}, error={}",
                    RagLogHelper.previewQuestion(query),
                    RagLogHelper.errorSummary(exception)
            );
            return Mono.just(List.of());
        });
    }

    private Mono<List<RagRetrievedChunk>> join(
            ExpandedState state,
            List<List<RagRetrievedChunk>> retrievalResults,
            int topK
    ) {
        return Mono.fromCallable(() -> retrievalMetrics.recordStage(
                        "join",
                        state.transformed().initial().hasFilter(),
                        () -> ragDocumentJoiner.join(retrievalResults, topK)
                ))
                .subscribeOn(ragBlockingScheduler);
    }

    private Mono<List<RagRetrievedChunk>> maybeProgressiveWiden(
            ExpandedState state,
            List<List<RagRetrievedChunk>> initialResults,
            List<RagRetrievedChunk> joined,
            RagRetrievalBudget initialBudget
    ) {
        if (!initialBudget.progressiveEnabled() || joined.size() >= initialBudget.answerTopK()) {
            return Mono.just(joined);
        }
        RagRequestFeedbacks.record(
                state.transformed().initial().feedbacks(),
                "retrieve",
                "progressive_widening",
                "初次召回上下文不足，已触发二次候选集放大。"
        );
        retrievalMetrics.recordFallback("retrieve", "progressive", "insufficient_contexts");
        RagRetrievalBudget retryBudget = retrievalBudgetPlanner.plan(
                state.transformed().initial().askRequest().question(),
                state.expandedQuery().retrievalQueries(),
                state.transformed().initial().filter(),
                state.transformed().initial().topK(),
                true
        );
        log.atInfo()
                .addKeyValue(RagLogFields.EVENT_NAME, "rag.ask.progressive_widening")
                .addKeyValue(RagLogFields.EVENT_OUTCOME, RagLogFields.OUTCOME_STARTED)
                .addKeyValue(RagLogFields.RAG_CORRELATION_ID, state.transformed().initial().correlationId())
                .addKeyValue(RagLogFields.RAG_CONTEXT_COUNT, joined.size())
                .addKeyValue(RagLogFields.RAG_TOP_K, initialBudget.answerTopK())
                .addKeyValue("rag.retry_per_query_top_k", retryBudget.perQueryTopK())
                .addKeyValue("rag.retry_semantic_candidate_top_k", retryBudget.semanticCandidateTopK())
                .addKeyValue("rag.retry_keyword_candidate_top_k", retryBudget.keywordCandidateTopK())
                .addKeyValue("rag.retrieval_budget_reason", retryBudget.reason())
                .log("RAG progressive widening triggered");
        return retrieveWithBudget(state, retryBudget)
                .flatMap(retryResults -> {
                    List<List<RagRetrievedChunk>> combined = new ArrayList<>(initialResults.size() + retryResults.size());
                    combined.addAll(initialResults);
                    combined.addAll(retryResults);
                    return join(state, combined, retryBudget.answerTopK());
                });
    }

    private Mono<List<RagRetrievedChunk>> expandNeighborWindow(
            ExpandedState state,
            List<RagRetrievedChunk> joinedContexts
    ) {
        return Mono.fromCallable(() -> retrievalMetrics.recordStage(
                        "neighbor_window",
                        state.transformed().initial().hasFilter(),
                        () -> expandNeighborWindow(joinedContexts)
                ))
                .subscribeOn(ragBlockingScheduler);
    }

    private List<RagRetrievedChunk> expandNeighborWindow(List<RagRetrievedChunk> contexts) {
        int before = ragProperties.retrieval().neighborWindowBefore();
        int after = ragProperties.retrieval().neighborWindowAfter();
        if (contexts == null || contexts.isEmpty() || (before <= 0 && after <= 0)) {
            return contexts;
        }

        LinkedHashMap<String, RagRetrievedChunk> expanded = new LinkedHashMap<>();
        for (RagRetrievedChunk seed : contexts) {
            int seedIndex = seed.chunkIndex() == null ? 0 : seed.chunkIndex();
            int start = Math.max(0, seedIndex - Math.max(before, 0));
            int end = seedIndex + Math.max(after, 0);
            List<RagChunkSearchView> window = indexingChunkQueryFacade.findActiveChunksByDocumentIdAndRange(
                    seed.documentId(),
                    start,
                    end
            );

            if (window.isEmpty()) {
                expanded.putIfAbsent(chunkKey(seed.chunkId(), seed.documentId(), seed.chunkIndex()), seed);
                continue;
            }

            for (RagChunkSearchView row : window) {
                if (seed.chunkId() != null && seed.chunkId().equals(row.chunkId())) {
                    expanded.putIfAbsent(chunkKey(seed.chunkId(), seed.documentId(), seed.chunkIndex()), seed);
                    continue;
                }

                RagChunkMetadataView metadataView = parseMetadata(row);
                int distance = Math.abs((row.chunkIndex() == null ? seedIndex : row.chunkIndex()) - seedIndex);
                double baseScore = seed.score() == null ? 0.0d : seed.score();
                RagRetrievedChunk neighbor = new RagRetrievedChunk(
                        row.chunkId(),
                        row.documentId(),
                        row.title(),
                        row.sourceType(),
                        row.sourceUri(),
                        row.chunkIndex(),
                        Math.max(0.0d, baseScore - (distance * 0.0001d)),
                        row.chunkText(),
                        metadataView.headingPath(),
                        metadataView.blockType(),
                        metadataView.codeLanguage()
                );
                expanded.putIfAbsent(
                        chunkKey(neighbor.chunkId(), neighbor.documentId(), neighbor.chunkIndex()),
                        neighbor
                );
            }
        }

        log.debug(
                "RAG neighbor expansion completed: seedCount={}, expandedCount={}, before={}, after={}",
                contexts.size(),
                expanded.size(),
                before,
                after
        );
        return new ArrayList<>(expanded.values());
    }

    private RagChunkMetadataView parseMetadata(RagChunkSearchView row) {
        Map<String, Object> raw = row.metadata() == null ? Map.of() : row.metadata();
        if (raw.isEmpty() && metadataHelper != null) {
            return metadataHelper.parse(null);
        }
        return new RagChunkMetadataView(
                asText(raw.get("blockType")),
                asText(raw.get("codeLanguage")),
                toStringList(raw.get("headingPath")),
                toStringList(raw.get("documentTags")),
                raw
        );
    }

    private List<String> toStringList(Object value) {
        if (metadataHelper != null) {
            return metadataHelper.toStringList(value);
        }
        if (!(value instanceof List<?> rawList)) {
            return List.of();
        }
        return rawList.stream()
                .filter(item -> item != null && !String.valueOf(item).trim().isEmpty())
                .map(String::valueOf)
                .toList();
    }

    private String asText(Object value) {
        if (value == null) {
            return null;
        }
        String text = String.valueOf(value).trim();
        return text.isEmpty() ? null : text;
    }

    private void logPreprocessed(
            TransformedState transformedState,
            RagQueryExpansionResult expandedQuery,
            RagRetrievalBudget budget
    ) {
        if (log.isDebugEnabled()) {
            log.atDebug()
                    .addKeyValue(RagLogFields.EVENT_NAME, "rag.ask.preprocessed")
                    .addKeyValue(RagLogFields.EVENT_OUTCOME, RagLogFields.OUTCOME_SUCCESS)
                    .addKeyValue(RagLogFields.RAG_CORRELATION_ID, transformedState.initial().correlationId())
                    .addKeyValue("rag.conversation_turns", transformedState.transformedQuery().conversationTurns())
                    .addKeyValue("rag.query_transformed", transformedState.transformedQuery().queryTransformed())
                    .addKeyValue("rag.transformed_by_model", transformedState.transformedQuery().transformedByModel())
                    .addKeyValue(RagLogFields.RAG_QUERY_COUNT, expandedQuery.retrievalQueries().size())
                    .addKeyValue(RagLogFields.RAG_EXPANDED_BY_MODEL, expandedQuery.expandedByModel())
                    .addKeyValue("rag.answer_top_k", budget.answerTopK())
                    .addKeyValue("rag.per_query_top_k", budget.perQueryTopK())
                    .addKeyValue("rag.semantic_candidate_top_k", budget.semanticCandidateTopK())
                    .addKeyValue("rag.keyword_candidate_top_k", budget.keywordCandidateTopK())
                    .addKeyValue("rag.retrieval_budget_reason", budget.reason())
                    .addKeyValue(RagLogFields.RAG_QUERY_EXPANDED, expandedQuery.retrievalQueries())
                    .addKeyValue("rag.retrieval_queries", expandedQuery.retrievalQueries().stream()
                            .map(RagLogHelper::previewQuestion)
                            .toList())
                    .log("RAG query preprocessing completed");
        }
    }

    private boolean isTimeout(Throwable exception) {
        Throwable current = exception;
        while (current != null) {
            if (current instanceof TimeoutException) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private String chunkKey(Long chunkId, Long documentId, Integer chunkIndex) {
        if (chunkId != null) {
            return "chunk:" + chunkId;
        }
        return "document:" + documentId + ":" + chunkIndex;
    }

    private record TransformedState(
            RagQueryHarnessRequest initial,
            RagQueryTransformationResult transformedQuery
    ) {
    }

    private record ExpandedState(
            TransformedState transformed,
            RagQueryExpansionResult expandedQuery,
            RagRetrievalBudget budget
    ) {
    }
}
