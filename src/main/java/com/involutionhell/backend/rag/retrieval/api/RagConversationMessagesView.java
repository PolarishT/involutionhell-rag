package com.involutionhell.backend.rag.retrieval.api;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.List;

public record RagConversationMessagesView(
        String conversationId,
        List<RagConversationMessageView> messages,
        @JsonInclude(JsonInclude.Include.NON_NULL)
        String nextCursor
) {
}
