package com.involutionhell.backend.rag.retrieval.service;

/**
 * 查询压缩/改写结果。
 *
 * @param originalQuestion 原始用户问题
 * @param retrievalQuestion 实际进入后续扩写/检索的 query
 * @param queryTransformed 是否发生过查询改写
 * @param transformedByModel 是否由模型完成了改写
 * @param conversationTurns 当前对话总轮数
 * @param rewriteConfidence 原 query 不经 rewrite 也适合检索的置信度
 * @param rewriteDecision rewrite 门控决策
 * @param rewriteConfidenceReason rewrite confidence 判定或降级原因
 */
public record RagQueryTransformationResult(
        String originalQuestion,
        String retrievalQuestion,
        boolean queryTransformed,
        boolean transformedByModel,
        int conversationTurns,
        Double rewriteConfidence,
        String rewriteDecision,
        String rewriteConfidenceReason
) {
    public RagQueryTransformationResult(
            String originalQuestion,
            String retrievalQuestion,
            boolean queryTransformed,
            boolean transformedByModel,
            int conversationTurns
    ) {
        this(
                originalQuestion,
                retrievalQuestion,
                queryTransformed,
                transformedByModel,
                conversationTurns,
                null,
                "skipped_disabled",
                null
        );
    }
}
