package com.involutionhell.backend.rag.indexing.persistence.jdbc;

import com.involutionhell.backend.rag.indexing.persistence.RagEmbeddingCacheDraft;
import com.involutionhell.backend.rag.indexing.persistence.RagEmbeddingCacheRepository;
import com.involutionhell.backend.rag.shared.persistence.RagJdbcRows;
import com.involutionhell.backend.rag.shared.persistence.RagNestedTransactionExecutor;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
public class JdbcRagEmbeddingCacheRepository implements RagEmbeddingCacheRepository {

    private final JdbcClient jdbcClient;
    private final RagNestedTransactionExecutor nestedTransactionExecutor;

    public JdbcRagEmbeddingCacheRepository(
            JdbcClient jdbcClient,
            RagNestedTransactionExecutor nestedTransactionExecutor
    ) {
        this.jdbcClient = jdbcClient;
        this.nestedTransactionExecutor = nestedTransactionExecutor;
    }

    @Override
    public Map<String, String> findEmbeddingJsonByChunkHashes(
            List<String> chunkHashes,
            String embeddingModel,
            int embeddingDimension
    ) {
        if (chunkHashes == null || chunkHashes.isEmpty()) {
            return Map.of();
        }
        List<CacheEntry> entries = jdbcClient.sql("""
                SELECT chunk_hash, embedding_json
                FROM rag_embedding_cache
                WHERE embedding_model = :embeddingModel
                  AND embedding_dimension = :embeddingDimension
                  AND chunk_hash IN (:chunkHashes)
                """)
                .param("embeddingModel", embeddingModel)
                .param("embeddingDimension", embeddingDimension)
                .param("chunkHashes", chunkHashes)
                .query(this::mapEntry)
                .list();
        Map<String, String> results = new LinkedHashMap<>();
        for (CacheEntry entry : entries) {
            results.put(entry.chunkHash(), entry.embeddingJson());
        }
        return results;
    }

    @Override
    public void saveAll(List<RagEmbeddingCacheDraft> drafts) {
        if (drafts == null || drafts.isEmpty()) {
            return;
        }
        for (RagEmbeddingCacheDraft draft : drafts) {
            nestedTransactionExecutor.executeWithIntegrityRetry(() -> upsertOnce(draft));
        }
    }

    private void upsertOnce(RagEmbeddingCacheDraft draft) {
        int updated = jdbcClient.sql("""
                UPDATE rag_embedding_cache
                SET embedding_json = :embeddingJson, updated_at = now()
                WHERE chunk_hash = :chunkHash
                  AND embedding_model = :embeddingModel
                  AND embedding_dimension = :embeddingDimension
                """)
                .param("embeddingJson", draft.embeddingJson())
                .param("chunkHash", draft.chunkHash())
                .param("embeddingModel", draft.embeddingModel())
                .param("embeddingDimension", draft.embeddingDimension())
                .update();
        if (updated > 0) {
            return;
        }
        jdbcClient.sql("""
                INSERT INTO rag_embedding_cache (
                    chunk_hash, embedding_model, embedding_dimension, embedding_json
                ) VALUES (
                    :chunkHash, :embeddingModel, :embeddingDimension, :embeddingJson
                )
                """)
                .param("chunkHash", draft.chunkHash())
                .param("embeddingModel", draft.embeddingModel())
                .param("embeddingDimension", draft.embeddingDimension())
                .param("embeddingJson", draft.embeddingJson())
                .update();
    }

    private CacheEntry mapEntry(ResultSet resultSet, int rowNumber) throws SQLException {
        return new CacheEntry(
                RagJdbcRows.string(resultSet, "chunk_hash"),
                RagJdbcRows.string(resultSet, "embedding_json")
        );
    }

    private record CacheEntry(String chunkHash, String embeddingJson) {
    }
}
