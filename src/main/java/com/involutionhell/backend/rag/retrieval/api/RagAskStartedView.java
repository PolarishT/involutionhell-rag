package com.involutionhell.backend.rag.retrieval.api;

/**
 * RAG 问答流开始事件。
 */
public record RagAskStartedView(
        String correlationId,
        String conversationId,
        String question,
        int topK
) {
}
