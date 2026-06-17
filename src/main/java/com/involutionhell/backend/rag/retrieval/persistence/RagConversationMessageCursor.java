package com.involutionhell.backend.rag.retrieval.persistence;

public record RagConversationMessageCursor(
        int sequenceNo,
        Long id
) {
}
