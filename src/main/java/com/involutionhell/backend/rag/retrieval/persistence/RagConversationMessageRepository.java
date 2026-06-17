package com.involutionhell.backend.rag.retrieval.persistence;

import java.util.List;

public interface RagConversationMessageRepository {

    RagConversationMessageRecord append(
            Long conversationId,
            String role,
            String content,
            String status,
            String correlationId
    );

    List<RagConversationMessageRecord> findByConversationId(Long conversationId);

    RagConversationMessagePage findByConversationId(Long conversationId, int limit, RagConversationMessageCursor cursor);

    List<RagConversationMessageRecord> findRecentByConversationId(Long conversationId, int limit);

    RagConversationMessageRecord findById(Long id);

    RagConversationMessageRecord updateContentAndStatus(Long id, String content, String status);
}
