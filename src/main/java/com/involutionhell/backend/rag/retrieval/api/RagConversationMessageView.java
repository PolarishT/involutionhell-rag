package com.involutionhell.backend.rag.retrieval.api;

import java.time.OffsetDateTime;

public record RagConversationMessageView(
        String id,
        String role,
        String content,
        OffsetDateTime createdAt
) {
}
