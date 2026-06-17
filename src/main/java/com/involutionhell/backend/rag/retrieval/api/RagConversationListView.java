package com.involutionhell.backend.rag.retrieval.api;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.List;

public record RagConversationListView(
        List<RagConversationSummaryView> conversations,
        @JsonInclude(JsonInclude.Include.NON_NULL)
        String nextCursor
) {
}
