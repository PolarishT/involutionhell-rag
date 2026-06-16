package com.involutionhell.backend.rag.retrieval.harness;

import com.involutionhell.backend.rag.retrieval.api.RagAskRequest;
import com.involutionhell.backend.rag.retrieval.api.RagResponseNoticeView;
import com.involutionhell.backend.rag.shared.metadata.RagSearchFilter;
import java.util.Set;

/**
 * 一次 query harness 执行所需的上下文。
 */
public record RagQueryHarnessRequest(
        RagAskRequest askRequest,
        String correlationId,
        int topK,
        RagSearchFilter filter,
        boolean hasFilter,
        Set<RagResponseNoticeView> feedbacks
) {
}
