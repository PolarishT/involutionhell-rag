package com.involutionhell.backend.rag.retrieval.harness;

import com.involutionhell.backend.rag.retrieval.model.RagRetrievedChunk;
import com.involutionhell.backend.rag.retrieval.service.RagQueryExpansionResult;
import com.involutionhell.backend.rag.retrieval.service.RagQueryTransformationResult;
import java.util.List;

/**
 * Query Harness 运行时产生的 typed stage event。
 */
public record RagQueryHarnessEvent(
        RagQueryHarnessStage stage,
        RagQueryTransformationResult transformedQuery,
        RagQueryExpansionResult expandedQuery,
        List<RagRetrievedChunk> contexts
) {
    public RagQueryHarnessEvent {
        contexts = contexts == null ? List.of() : List.copyOf(contexts);
    }

    static RagQueryHarnessEvent transformed(RagQueryTransformationResult transformedQuery) {
        return new RagQueryHarnessEvent(
                RagQueryHarnessStage.TRANSFORMED,
                transformedQuery,
                null,
                List.of()
        );
    }

    static RagQueryHarnessEvent expanded(
            RagQueryTransformationResult transformedQuery,
            RagQueryExpansionResult expandedQuery
    ) {
        return new RagQueryHarnessEvent(
                RagQueryHarnessStage.EXPANDED,
                transformedQuery,
                expandedQuery,
                List.of()
        );
    }

    static RagQueryHarnessEvent contexts(
            RagQueryTransformationResult transformedQuery,
            RagQueryExpansionResult expandedQuery,
            List<RagRetrievedChunk> contexts
    ) {
        return new RagQueryHarnessEvent(
                RagQueryHarnessStage.CONTEXTS,
                transformedQuery,
                expandedQuery,
                contexts
        );
    }
}
