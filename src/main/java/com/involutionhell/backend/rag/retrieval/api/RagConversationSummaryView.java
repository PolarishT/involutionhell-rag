package com.involutionhell.backend.rag.retrieval.api;

import java.time.OffsetDateTime;

public record RagConversationSummaryView(
        String conversationId,
        String title,
        OffsetDateTime createdAt,
        OffsetDateTime updatedAt
) {
}
