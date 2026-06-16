package com.involutionhell.backend.rag.document.persistence.mybatis;

import com.involutionhell.backend.rag.document.persistence.RagDocumentRecord;
import com.involutionhell.backend.rag.document.persistence.RagDocumentRepository;
import com.involutionhell.backend.rag.shared.model.RagDocumentStatus;
import com.involutionhell.backend.rag.shared.persistence.RagDatabaseDialect;
import com.involutionhell.backend.rag.shared.persistence.RagFlexJson;
import com.involutionhell.backend.rag.shared.support.RagJsonCodec;
import com.mybatisflex.core.query.QueryChain;
import com.mybatisflex.core.query.QueryColumn;
import com.mybatisflex.core.row.DbChain;
import com.mybatisflex.core.update.UpdateChain;
import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import static com.mybatisflex.core.query.QueryMethods.coalesce;

@Repository
public class MyBatisFlexRagDocumentRepository implements RagDocumentRepository {

    private static final QueryColumn UPDATED_AT = new QueryColumn("updated_at");
    private static final QueryColumn LAST_ATTEMPTED_AT = new QueryColumn("last_attempted_at");

    private final RagDocumentMapper documentMapper;
    private final RagJsonCodec jsonCodec;
    private final RagDatabaseDialect databaseDialect;

    public MyBatisFlexRagDocumentRepository(
            RagDocumentMapper documentMapper,
            RagJsonCodec jsonCodec,
            RagDatabaseDialect databaseDialect
    ) {
        this.documentMapper = documentMapper;
        this.jsonCodec = jsonCodec;
        this.databaseDialect = databaseDialect;
    }

    @Override
    @Transactional
    public RagDocumentRecord save(
            String sourceType,
            String sourceUri,
            String externalRef,
            String title,
            String content,
            String contentSha256,
            Map<String, Object> metadata
    ) {
        DbChain insert = DbChain.table("rag_documents")
                .set("source_type", sourceType)
                .set("source_uri", sourceUri)
                .set("external_ref", externalRef)
                .set("title", title)
                .set("content", content)
                .set("content_sha256", contentSha256)
                .set("status", RagDocumentStatus.PENDING.name());
        RagFlexJson.setJson(insert, databaseDialect, "metadata", jsonCodec.write(metadata == null ? Map.of() : metadata))
                .save();
        return findLatestMatching(sourceType, sourceUri, externalRef, title, contentSha256)
                .orElseThrow(() -> new IllegalStateException("创建 RAG 文档后查询失败"));
    }

    @Override
    @Transactional
    public RagDocumentRecord update(
            Long id,
            String sourceType,
            String sourceUri,
            String externalRef,
            String title,
            String content,
            String contentSha256,
            Map<String, Object> metadata
    ) {
        UpdateChain<RagDocumentEntity> update = UpdateChain.of(documentMapper)
                .set("source_type", sourceType, true)
                .set("source_uri", sourceUri, true)
                .set("external_ref", externalRef, true)
                .set("title", title, true)
                .set("content", content, true)
                .set("content_sha256", contentSha256, true)
                .set("status", RagDocumentStatus.PENDING.name(), true)
                .set("attempt_count", 0, true);
        boolean updated = RagFlexJson.setJson(
                        update,
                        databaseDialect,
                        "metadata",
                        jsonCodec.write(metadata == null ? Map.of() : metadata)
                )
                .setRaw("last_error", "NULL", true)
                .setRaw("last_attempted_at", "NULL", true)
                .setRaw("updated_at", "now()", true)
                .where("id = ? AND status <> ?", id, RagDocumentStatus.DELETING.name())
                .update();
        if (!updated) {
            throw new IllegalArgumentException("RAG 文档不存在: " + id);
        }
        return findById(id).orElseThrow(() -> new IllegalStateException("更新 RAG 文档后查询失败"));
    }

    @Override
    public Optional<RagDocumentRecord> findById(Long id) {
        return QueryChain.of(documentMapper)
                .where("id = ?", id)
                .limit(1)
                .oneOpt()
                .map(this::toRecord);
    }

    @Override
    public List<RagDocumentRecord> findPendingBefore(OffsetDateTime cutoff, int limit) {
        return findByStatusBefore(RagDocumentStatus.PENDING, UPDATED_AT, cutoff, limit);
    }

    @Override
    public List<RagDocumentRecord> findProcessingBefore(OffsetDateTime cutoff, int limit) {
        return findByStatusBefore(RagDocumentStatus.PROCESSING, coalesce(LAST_ATTEMPTED_AT, UPDATED_AT), cutoff, limit);
    }

    @Override
    public List<RagDocumentRecord> findFailedBefore(OffsetDateTime cutoff, int limit) {
        return findByStatusBefore(RagDocumentStatus.FAILED, UPDATED_AT, cutoff, limit);
    }

    @Override
    public List<RagDocumentRecord> findDeletingBefore(OffsetDateTime cutoff, int limit) {
        return findByStatusBefore(RagDocumentStatus.DELETING, UPDATED_AT, cutoff, limit);
    }

    @Override
    public void markPending(Long id) {
        updateUnlessDeleting(id, UpdateChain.of(documentMapper)
                .set("status", RagDocumentStatus.PENDING.name(), true)
                .set("attempt_count", 0, true)
                .setRaw("last_error", "NULL", true)
                .setRaw("last_attempted_at", "NULL", true)
                .setRaw("updated_at", "now()", true)
                .where("id = ? AND status <> ?", id, RagDocumentStatus.DELETING.name()));
    }

    @Override
    public void requeue(Long id, String note) {
        updateUnlessDeleting(id, UpdateChain.of(documentMapper)
                .set("status", RagDocumentStatus.PENDING.name(), true)
                .set("last_error", note, true)
                .setRaw("updated_at", "now()", true)
                .where("id = ? AND status <> ?", id, RagDocumentStatus.DELETING.name()));
    }

    @Override
    public void markProcessing(Long id) {
        updateUnlessDeleting(id, UpdateChain.of(documentMapper)
                .set("status", RagDocumentStatus.PROCESSING.name(), true)
                .setRaw("attempt_count", "attempt_count + 1", true)
                .setRaw("last_error", "NULL", true)
                .setRaw("last_attempted_at", "now()", true)
                .setRaw("updated_at", "now()", true)
                .where("id = ? AND status <> ?", id, RagDocumentStatus.DELETING.name()));
    }

    @Override
    public void markIndexed(Long id, Long indexedGeneration, int chunkCount, OffsetDateTime indexedAt) {
        updateUnlessDeleting(id, UpdateChain.of(documentMapper)
                .set("status", RagDocumentStatus.INDEXED.name(), true)
                .set("indexed_generation", indexedGeneration, true)
                .set("chunk_count", chunkCount, true)
                .set("indexed_at", indexedAt, true)
                .setRaw("last_error", "NULL", true)
                .setRaw("updated_at", "now()", true)
                .where("id = ? AND status <> ?", id, RagDocumentStatus.DELETING.name()));
    }

    @Override
    public void markFailed(Long id, String errorMessage) {
        updateUnlessDeleting(id, UpdateChain.of(documentMapper)
                .set("status", RagDocumentStatus.FAILED.name(), true)
                .set("last_error", errorMessage, true)
                .setRaw("updated_at", "now()", true)
                .where("id = ? AND status <> ?", id, RagDocumentStatus.DELETING.name()));
    }

    @Override
    public void markDeleting(Long id, String note) {
        updateRequired(id, UpdateChain.of(documentMapper)
                .set("status", RagDocumentStatus.DELETING.name(), true)
                .set("last_error", note, true)
                .setRaw("updated_at", "now()", true)
                .where("id = ?", id));
    }

    @Override
    @Transactional
    public void deleteById(Long id) {
        DbChain.table("rag_chunks").where("document_id = ?", id).remove();
        DbChain.table("rag_index_outbox").where("document_id = ?", id).remove();
        DbChain.table("rag_index_job_transitions").where("document_id = ?", id).remove();
        DbChain.table("rag_index_jobs").where("document_id = ?", id).remove();
        boolean deleted = DbChain.table("rag_documents").where("id = ?", id).remove();
        if (!deleted) {
            throw new IllegalArgumentException("RAG 文档不存在: " + id);
        }
    }

    private List<RagDocumentRecord> findByStatusBefore(
            RagDocumentStatus status,
            QueryColumn cutoffColumn,
            OffsetDateTime cutoff,
            int limit
    ) {
        return QueryChain.of(documentMapper)
                .where("status = ?", status.name())
                .and(cutoffColumn.le(Timestamp.from(cutoff.toInstant())))
                .orderBy(cutoffColumn.asc())
                .limit(limit)
                .list()
                .stream()
                .map(this::toRecord)
                .toList();
    }

    private Optional<RagDocumentRecord> findLatestMatching(
            String sourceType,
            String sourceUri,
            String externalRef,
            String title,
            String contentSha256
    ) {
        return QueryChain.of(documentMapper)
                .where("source_type = ?", sourceType)
                .and("source_uri IS NOT DISTINCT FROM ?", sourceUri)
                .and("external_ref IS NOT DISTINCT FROM ?", externalRef)
                .and("title IS NOT DISTINCT FROM ?", title)
                .and("content_sha256 = ?", contentSha256)
                .orderBy("id DESC")
                .limit(1)
                .oneOpt()
                .map(this::toRecord);
    }

    private void updateRequired(Long id, UpdateChain<RagDocumentEntity> update) {
        if (!update.update()) {
            throw new IllegalArgumentException("RAG 文档不存在: " + id);
        }
    }

    private void updateUnlessDeleting(Long id, UpdateChain<RagDocumentEntity> update) {
        if (update.update()) {
            return;
        }
        Optional<RagDocumentRecord> current = findById(id);
        if (current.isPresent() && RagDocumentStatus.DELETING.name().equals(current.get().status())) {
            return;
        }
        throw new IllegalArgumentException("RAG 文档不存在: " + id);
    }

    private RagDocumentRecord toRecord(RagDocumentEntity entity) {
        return new RagDocumentRecord(
                entity.getId(),
                entity.getSourceType(),
                entity.getSourceUri(),
                entity.getExternalRef(),
                entity.getTitle(),
                entity.getContent(),
                entity.getContentSha256(),
                entity.getIndexedGeneration(),
                entity.getStatus(),
                entity.getChunkCount() == null ? 0 : entity.getChunkCount(),
                entity.getAttemptCount() == null ? 0 : entity.getAttemptCount(),
                jsonCodec.readMap(entity.getMetadata()),
                entity.getLastError(),
                entity.getLastAttemptedAt(),
                entity.getIndexedAt(),
                entity.getCreatedAt(),
                entity.getUpdatedAt()
        );
    }
}
