package com.involutionhell.backend.rag.retrieval.persistence;

import java.util.List;

public record RagConversationMessagePage(
        List<RagConversationMessageRecord> items,
        RagConversationMessageCursor nextCursor
) {
}
