package com.involutionhell.backend.rag.retrieval.application;

import com.involutionhell.backend.rag.retrieval.api.*;
import com.involutionhell.backend.rag.retrieval.harness.RagQueryHarness;
import com.involutionhell.backend.rag.retrieval.harness.RagQueryHarnessEvent;
import com.involutionhell.backend.rag.retrieval.harness.RagQueryHarnessRequest;
import com.involutionhell.backend.rag.retrieval.harness.RagQueryHarnessStage;
import com.involutionhell.backend.rag.retrieval.model.RagRetrievedChunk;
import com.involutionhell.backend.rag.retrieval.observability.RagRetrievalMetrics;
import com.involutionhell.backend.rag.retrieval.service.RagAnswerGenerator;
import com.involutionhell.backend.rag.retrieval.service.RagQueryExpansionResult;
import com.involutionhell.backend.rag.retrieval.service.RagQueryTransformationResult;
import com.involutionhell.backend.rag.retrieval.support.RagRequestFeedbacks;
import com.involutionhell.backend.rag.shared.metadata.RagSearchFilter;
import com.involutionhell.backend.rag.shared.properties.RagProperties;
import com.involutionhell.backend.rag.shared.support.RagLogFields;
import com.involutionhell.backend.rag.shared.support.RagLogHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.scheduler.Scheduler;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@Service
class RagAskService implements RagAskFacade {

    private static final Logger log = LoggerFactory.getLogger(RagAskService.class);

    private final RagQueryHarness queryHarness;
    private final RagAnswerGenerator answerGenerator;
    private final RagProperties ragProperties;
    private final RagRetrievalMetrics retrievalMetrics;
    private final Scheduler ragBlockingScheduler;
    private final RagConversationService conversationService;

    RagAskService(
            RagQueryHarness queryHarness,
            RagAnswerGenerator answerGenerator,
            RagProperties ragProperties,
            RagRetrievalMetrics retrievalMetrics,
            @Qualifier("ragBlockingScheduler") Scheduler ragBlockingScheduler,
            RagConversationService conversationService
    ) {
        this.queryHarness = queryHarness;
        this.answerGenerator = answerGenerator;
        this.ragProperties = ragProperties;
        this.retrievalMetrics = retrievalMetrics;
        this.ragBlockingScheduler = ragBlockingScheduler;
        this.conversationService = conversationService;
    }

    @Override
    public Flux<RagAskStreamEvent> askStream(RagAskRequest request) {
        return Flux.defer(() -> doAskStream(request))
                .subscribeOn(ragBlockingScheduler)
                .publishOn(ragBlockingScheduler);
    }

    private Flux<RagAskStreamEvent> doAskStream(RagAskRequest request) {
        String requestedCorrelationId = "ask:" + UUID.randomUUID();
        String runId = "run:" + UUID.randomUUID();

        int topK = request.topK() == null ? ragProperties.defaultTopK() : request.topK();

        RagSearchFilter filter = RagSearchFilter.of(
                request.sourceUriPrefix(),
                request.tags(),
                request.headingPathContains()
        );

        RagConversationService.AskConversationState conversationState = conversationService.beginAsk(
                runId,
                requestedCorrelationId,
                request.userId(),
                request.conversationId(),
                request.question(),
                request.requestId(),
                topK,
                filter
        );

        String correlationId = conversationState.correlationId();
        Set<RagResponseNoticeView> feedbacks = RagRequestFeedbacks.begin();
        AtomicInteger sequence = new AtomicInteger();
        AtomicBoolean generatedByModel = new AtomicBoolean(false);
        AtomicBoolean askFinalized = new AtomicBoolean(false);
        StringBuilder answer = new StringBuilder();

        AskInitialState initialState = initialize(
                requestWithHistory(request, conversationState.history()),
                correlationId,
                topK,
                filter,
                feedbacks,
                conversationState
        );

        RagAskStreamEvent started = event(
                sequence,
                "started",
                correlationId,
                new RagAskStartedView(
                        correlationId,
                        conversationState.conversation().conversationId(),
                        request.question(),
                        initialState.topK()
                )
        );

        if (conversationState.existing()) {
            return replayExistingAsk(
                    sequence,
                    correlationId,
                    request.question(),
                    initialState.topK(),
                    conversationState
            );
        }

        return Flux.concat(
                        Flux.just(started),
                        queryHarness.execute(new RagQueryHarnessRequest(
                                        initialState.request(),
                                        initialState.correlationId(),
                                        initialState.topK(),
                                        initialState.filter(),
                                        initialState.hasFilter(),
                                        initialState.feedbacks()
                                ))
                                .subscribeOn(ragBlockingScheduler)
                                .publishOn(ragBlockingScheduler)
                                .concatMap(harnessEvent -> toAskEvents(
                                        sequence,
                                        initialState,
                                        harnessEvent,
                                        generatedByModel,
                                        answer,
                                        askFinalized
                                ))
                )
                .onErrorResume(exception -> handleAskError(
                        sequence,
                        correlationId,
                        runId,
                        conversationState,
                        feedbacks,
                        askFinalized,
                        exception
                ))
                .doFinally(signalType -> {
                    if (signalType == SignalType.CANCEL && askFinalized.compareAndSet(false, true)) {
                        failAskSafelyAsync(
                                conversationState,
                                new CancellationException("RAG ask stream cancelled by client"),
                                feedbacks
                        ).subscribe();
                    }
                });
    }

    private Mono<RagAskStreamEvent> handleAskError(
            AtomicInteger sequence,
            String correlationId,
            String runId,
            RagConversationService.AskConversationState conversationState,
            Set<RagResponseNoticeView> feedbacks,
            AtomicBoolean askFinalized,
            Throwable exception
    ) {
        return Mono.fromCallable(() -> {
                    log.atError()
                            .addKeyValue(RagLogFields.EVENT_NAME, "rag.ask.failed")
                            .addKeyValue(RagLogFields.EVENT_OUTCOME, RagLogFields.OUTCOME_FAILURE)
                            .addKeyValue(RagLogFields.RAG_CORRELATION_ID, correlationId)
                            .addKeyValue("rag.conversation_id", conversationState.conversation().conversationId())
                            .addKeyValue("rag.run_id", runId)
                            .addKeyValue(RagLogFields.RAG_ERROR_SUMMARY, RagLogHelper.errorSummary(exception))
                            .setCause(exception)
                            .log("RAG ask stream failed");

                    RagAskStreamEvent errorEvent = toErrorEvent(sequence, correlationId, feedbacks, exception);

                    if (askFinalized.compareAndSet(false, true)) {
                        failAskSafely(conversationState, exception, feedbacks);
                    }

                    return errorEvent;
                })
                .subscribeOn(ragBlockingScheduler);
    }

    private AskInitialState initialize(
            RagAskRequest request,
            String correlationId,
            int topK,
            RagSearchFilter filter,
            Set<RagResponseNoticeView> feedbacks,
            RagConversationService.AskConversationState conversationState
    ) {
        boolean hasFilter = !filter.isEmpty();
        retrievalMetrics.recordRequest("ask");

        log.atInfo()
                .addKeyValue(RagLogFields.EVENT_NAME, "rag.ask.started")
                .addKeyValue(RagLogFields.EVENT_OUTCOME, RagLogFields.OUTCOME_STARTED)
                .addKeyValue(RagLogFields.RAG_CORRELATION_ID, correlationId)
                .addKeyValue(RagLogFields.RAG_QUESTION_LENGTH, request.question() == null ? 0 : request.question().length())
                .addKeyValue(RagLogFields.RAG_QUESTION_PREVIEW, RagLogHelper.previewQuestion(request.question()))
                .addKeyValue(RagLogFields.RAG_TOP_K, topK)
                .addKeyValue(RagLogFields.RAG_HISTORY_TURNS, request.history() == null ? 0 : request.history().size())
                .addKeyValue(RagLogFields.RAG_HAS_FILTER, hasFilter)
                .log("RAG ask started");

        return new AskInitialState(
                request,
                correlationId,
                topK,
                filter,
                hasFilter,
                feedbacks,
                conversationState
        );
    }

    private RagAskRequest requestWithHistory(RagAskRequest request, List<RagConversationMessage> history) {
        return new RagAskRequest(
                request.userId(),
                request.conversationId(),
                request.question(),
                request.topK(),
                request.sourceUriPrefix(),
                request.tags(),
                request.headingPathContains(),
                history,
                request.requestId()
        );
    }

    private Flux<RagAskStreamEvent> replayExistingAsk(
            AtomicInteger sequence,
            String correlationId,
            String question,
            int topK,
            RagConversationService.AskConversationState conversationState
    ) {
        RagAskStreamEvent started = event(
                sequence,
                "started",
                correlationId,
                new RagAskStartedView(
                        correlationId,
                        conversationState.conversation().conversationId(),
                        question,
                        topK
                )
        );

        if ("SUCCEEDED".equals(conversationState.runStatus())) {
            return Flux.concat(
                    Flux.just(started),
                    Flux.just(event(
                            sequence,
                            "answer_delta",
                            correlationId,
                            new RagAnswerDeltaView(conversationState.assistantMessage().content())
                    )),
                    Flux.just(event(
                            sequence,
                            "completed",
                            correlationId,
                            new RagAskCompletedView(false, false, List.of(), 0)
                    ))
            );
        }

        if ("RUNNING".equals(conversationState.runStatus())) {
            return Flux.just(
                    started,
                    event(
                            sequence,
                            "error",
                            correlationId,
                            new RagStreamErrorView(
                                    "duplicate_running",
                                    "同一 requestId 的问答仍在运行中。",
                                    true
                            )
                    )
            );
        }

        String errorCode = conversationState.errorCode() == null
                ? "duplicate_failed"
                : conversationState.errorCode();

        String errorMessage = conversationState.errorMessage() == null
                ? "同一 requestId 的问答已失败。"
                : conversationState.errorMessage();

        return Flux.just(
                started,
                event(
                        sequence,
                        "error",
                        correlationId,
                        new RagStreamErrorView(errorCode, errorMessage, true)
                )
        );
    }

    private Flux<RagAskStreamEvent> toAskEvents(
            AtomicInteger sequence,
            AskInitialState initialState,
            RagQueryHarnessEvent harnessEvent,
            AtomicBoolean generatedByModel,
            StringBuilder answer,
            AtomicBoolean askFinalized
    ) {
        if (harnessEvent.stage() == RagQueryHarnessStage.TRANSFORMED) {
            return Flux.just(toTransformedEvent(
                    sequence,
                    initialState.correlationId(),
                    harnessEvent.transformedQuery()
            ));
        }

        if (harnessEvent.stage() == RagQueryHarnessStage.EXPANDED) {
            return Flux.just(toExpandedEvent(
                    sequence,
                    initialState.correlationId(),
                    harnessEvent.expandedQuery()
            ));
        }

        return emitAnswerFlow(
                sequence,
                initialState,
                harnessEvent,
                generatedByModel,
                answer,
                askFinalized
        );
    }

    private Flux<RagAskStreamEvent> emitAnswerFlow(
            AtomicInteger sequence,
            AskInitialState initialState,
            RagQueryHarnessEvent queryResult,
            AtomicBoolean generatedByModel,
            StringBuilder answer,
            AtomicBoolean askFinalized
    ) {
        String correlationId = initialState.correlationId();
        RagAskRequest request = initialState.request();
        List<RagRetrievedChunk> contexts = queryResult.contexts();

        RagAskStreamEvent contextsEvent = event(
                sequence,
                "contexts",
                correlationId,
                new RagContextsView(contexts.stream().map(this::toContextView).toList())
        );

        Flux<RagAskStreamEvent> answerEvents = Flux.defer(() -> answerGenerator
                        .generateStream(
                                request.question(),
                                contexts,
                                initialState.feedbacks(),
                                generatedByModel::set
                        ))
                .subscribeOn(ragBlockingScheduler)
                .publishOn(ragBlockingScheduler)
                .concatMap(delta -> Mono.fromCallable(() -> {
                                    answer.append(delta);

                                    conversationService.streamAssistantAnswer(
                                            initialState.conversationState(),
                                            answer.toString()
                                    );

                                    return event(
                                            sequence,
                                            "answer_delta",
                                            correlationId,
                                            new RagAnswerDeltaView(delta)
                                    );
                                })
                                .subscribeOn(ragBlockingScheduler)
                );

        return Flux.concat(
                Flux.just(contextsEvent),
                answerEvents,
                noticeEvents(sequence, correlationId, initialState.feedbacks()),
                Mono.fromCallable(() -> completedEvent(
                                sequence,
                                initialState,
                                queryResult,
                                generatedByModel.get(),
                                answer.toString(),
                                askFinalized
                        ))
                        .subscribeOn(ragBlockingScheduler)
        );
    }

    private Flux<RagAskStreamEvent> noticeEvents(
            AtomicInteger sequence,
            String correlationId,
            Set<RagResponseNoticeView> feedbacks
    ) {
        return Flux.defer(() -> Flux.fromIterable(RagRequestFeedbacks.snapshot(feedbacks))
                .map(notice -> event(sequence, "notice", correlationId, notice)));
    }

    private RagAskStreamEvent completedEvent(
            AtomicInteger sequence,
            AskInitialState initialState,
            RagQueryHarnessEvent queryResult,
            boolean generatedByModel,
            String answer,
            AtomicBoolean askFinalized
    ) {
        String correlationId = initialState.correlationId();
        List<RagRetrievedChunk> contexts = queryResult.contexts();
        List<RagResponseNoticeView> notices = RagRequestFeedbacks.snapshot(initialState.feedbacks());
        boolean degraded = !notices.isEmpty();

        List<RagContextView> contextViews = contexts.stream()
                .map(this::toContextView)
                .toList();

        conversationService.completeAsk(
                initialState.conversationState(),
                answer,
                queryResult.transformedQuery().retrievalQuestion(),
                queryResult.expandedQuery().retrievalQueries(),
                contextViews,
                notices,
                generatedByModel,
                degraded,
                correlationId
        );

        askFinalized.set(true);

        log.atInfo()
                .addKeyValue(RagLogFields.EVENT_NAME, "rag.ask.completed")
                .addKeyValue(RagLogFields.EVENT_OUTCOME, RagLogFields.OUTCOME_SUCCESS)
                .addKeyValue(RagLogFields.RAG_CORRELATION_ID, correlationId)
                .addKeyValue(RagLogFields.RAG_QUERY_COUNT, queryResult.expandedQuery().retrievalQueries().size())
                .addKeyValue(RagLogFields.RAG_CONTEXT_COUNT, contexts.size())
                .addKeyValue(RagLogFields.RAG_GENERATED_BY_MODEL, generatedByModel)
                .addKeyValue(RagLogFields.RAG_QUERY_EXPANDED, queryResult.expandedQuery().queryExpanded())
                .addKeyValue(RagLogFields.RAG_EXPANDED_BY_MODEL, queryResult.expandedQuery().expandedByModel())
                .log("RAG ask completed");

        return event(
                sequence,
                "completed",
                correlationId,
                new RagAskCompletedView(
                        generatedByModel,
                        degraded,
                        notices,
                        contexts.size()
                )
        );
    }

    private RagAskStreamEvent toTransformedEvent(
            AtomicInteger sequence,
            String correlationId,
            RagQueryTransformationResult result
    ) {
        return event(
                sequence,
                "query_transformed",
                correlationId,
                new RagQueryTransformedView(
                        result.originalQuestion(),
                        result.retrievalQuestion(),
                        result.queryTransformed(),
                        result.transformedByModel(),
                        result.conversationTurns(),
                        result.rewriteConfidence(),
                        result.rewriteDecision(),
                        result.rewriteConfidenceReason()
                )
        );
    }

    private RagAskStreamEvent toExpandedEvent(
            AtomicInteger sequence,
            String correlationId,
            RagQueryExpansionResult result
    ) {
        return event(
                sequence,
                "query_expanded",
                correlationId,
                new RagQueryExpandedView(
                        result.originalQuestion(),
                        result.retrievalQueries(),
                        result.queryExpanded(),
                        result.expandedByModel()
                )
        );
    }

    private RagAskStreamEvent toErrorEvent(
            AtomicInteger sequence,
            String correlationId,
            Set<RagResponseNoticeView> feedbacks,
            Throwable exception
    ) {
        log.error("RAG ask stream failed: correlationId={}", correlationId, exception);

        RagRequestFeedbacks.record(
                feedbacks,
                "ask",
                "error",
                "问答链路发生异常，流已终止。"
        );

        return event(
                sequence,
                "error",
                correlationId,
                new RagStreamErrorView(
                        "error",
                        RagLogHelper.errorSummary(exception),
                        true
                )
        );
    }

    private Mono<Void> failAskSafelyAsync(
            RagConversationService.AskConversationState conversationState,
            Throwable exception,
            Set<RagResponseNoticeView> feedbacks
    ) {
        return Mono.fromRunnable(() -> failAskSafely(conversationState, exception, feedbacks))
                .subscribeOn(ragBlockingScheduler)
                .then();
    }

    private void failAskSafely(
            RagConversationService.AskConversationState conversationState,
            Throwable exception,
            Set<RagResponseNoticeView> feedbacks
    ) {
        try {
            conversationService.failAsk(
                    conversationState,
                    exception,
                    RagRequestFeedbacks.snapshot(feedbacks)
            );
        } catch (RuntimeException failure) {
            log.error(
                    "Failed to mark RAG ask as failed; stale ask recovery should repair it later: correlationId={}, error={}",
                    conversationState == null ? null : conversationState.correlationId(),
                    RagLogHelper.errorSummary(failure),
                    failure
            );
        }
    }

    private RagAskStreamEvent event(
            AtomicInteger sequence,
            String eventName,
            String correlationId,
            Object data
    ) {
        return new RagAskStreamEvent(
                correlationId + ":" + sequence.incrementAndGet(),
                eventName,
                correlationId,
                data
        );
    }

    private RagContextView toContextView(RagRetrievedChunk chunk) {
        return new RagContextView(
                chunk.chunkId(),
                chunk.documentId(),
                chunk.title(),
                chunk.sourceType(),
                chunk.sourceUri(),
                chunk.chunkIndex(),
                chunk.score(),
                chunk.content(),
                chunk.headingPath(),
                chunk.blockType(),
                chunk.codeLanguage()
        );
    }

    private record AskInitialState(
            RagAskRequest request,
            String correlationId,
            int topK,
            RagSearchFilter filter,
            boolean hasFilter,
            Set<RagResponseNoticeView> feedbacks,
            RagConversationService.AskConversationState conversationState
    ) {
    }
}