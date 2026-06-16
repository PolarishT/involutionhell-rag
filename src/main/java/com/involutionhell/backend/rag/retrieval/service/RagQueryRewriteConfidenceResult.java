package com.involutionhell.backend.rag.retrieval.service;

/**
 * Query rewrite 置信度判定结果。
 *
 * @param confidence 原 query 不经 rewrite 也适合检索的置信度，范围 0.0..1.0
 * @param reason 模型给出的简短原因，或内部降级原因
 * @param available 判定结果是否可用
 * @param fallbackReason 不可用时的机器可读原因
 */
public record RagQueryRewriteConfidenceResult(
        Double confidence,
        String reason,
        boolean available,
        String fallbackReason
) {

    public static RagQueryRewriteConfidenceResult available(double confidence, String reason) {
        return new RagQueryRewriteConfidenceResult(clamp(confidence), normalize(reason), true, null);
    }

    public static RagQueryRewriteConfidenceResult unavailable(String fallbackReason) {
        return new RagQueryRewriteConfidenceResult(null, normalize(fallbackReason), false, normalize(fallbackReason));
    }

    private static double clamp(double value) {
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            return 0.0d;
        }
        return Math.max(0.0d, Math.min(1.0d, value));
    }

    private static String normalize(String value) {
        if (value == null) {
            return null;
        }
        String normalized = value.trim();
        return normalized.isEmpty() ? null : normalized;
    }
}
