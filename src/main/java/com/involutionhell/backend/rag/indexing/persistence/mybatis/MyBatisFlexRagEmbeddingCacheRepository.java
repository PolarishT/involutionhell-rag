package com.involutionhell.backend.rag.indexing.persistence.mybatis;

import com.involutionhell.backend.rag.indexing.persistence.RagEmbeddingCacheDraft;
import com.involutionhell.backend.rag.indexing.persistence.RagEmbeddingCacheRepository;
import com.involutionhell.backend.rag.shared.persistence.RagNestedTransactionExecutor;
import com.mybatisflex.core.row.DbChain;
import com.mybatisflex.core.row.Row;
import com.mybatisflex.core.update.UpdateChain;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Repository;

@Repository
public class MyBatisFlexRagEmbeddingCacheRepository implements RagEmbeddingCacheRepository {

    private final RagEmbeddingCacheMapper cacheMapper;
    private final RagNestedTransactionExecutor nestedTransactionExecutor;

    public MyBatisFlexRagEmbeddingCacheRepository(
            RagEmbeddingCacheMapper cacheMapper,
            RagNestedTransactionExecutor nestedTransactionExecutor
    ) {
        this.cacheMapper = cacheMapper;
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
        String placeholders = String.join(",", Collections.nCopies(chunkHashes.size(), "?"));
        Object[] args = new Object[chunkHashes.size() + 2];
        args[0] = embeddingModel;
        args[1] = embeddingDimension;
        for (int index = 0; index < chunkHashes.size(); index++) {
            args[index + 2] = chunkHashes.get(index);
        }
        List<Row> rows = DbChain.table("rag_embedding_cache")
                .select("chunk_hash", "embedding_json")
                .where("embedding_model = ? AND embedding_dimension = ? AND chunk_hash IN (" + placeholders + ")", args)
                .list();
        Map<String, String> results = new LinkedHashMap<>();
        for (Row row : rows) {
            results.put(row.getString("chunk_hash"), row.getString("embedding_json"));
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
        boolean updated = UpdateChain.of(cacheMapper)
                .set("embedding_json", draft.embeddingJson(), true)
                .setRaw("updated_at", "now()", true)
                .where("chunk_hash = ? AND embedding_model = ? AND embedding_dimension = ?",
                        draft.chunkHash(), draft.embeddingModel(), draft.embeddingDimension())
                .update();
        if (updated) {
            return;
        }
        DbChain.table("rag_embedding_cache")
                .set("chunk_hash", draft.chunkHash())
                .set("embedding_model", draft.embeddingModel())
                .set("embedding_dimension", draft.embeddingDimension())
                .set("embedding_json", draft.embeddingJson())
                .save();
    }
}
