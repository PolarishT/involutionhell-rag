package com.involutionhell.backend.rag.indexing.persistence.mybatis;

import com.involutionhell.backend.rag.indexing.model.RagIndexJobStatus;
import com.involutionhell.backend.rag.indexing.model.RagIndexStage;
import com.involutionhell.backend.rag.indexing.persistence.RagIndexJobRecord;
import com.involutionhell.backend.rag.indexing.persistence.RagIndexJobRepository;
import com.involutionhell.backend.rag.shared.persistence.RagNestedTransactionExecutor;
import com.mybatisflex.core.query.QueryChain;
import com.mybatisflex.core.row.DbChain;
import com.mybatisflex.core.update.UpdateChain;
import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import org.springframework.stereotype.Repository;

@Repository
public class MyBatisFlexRagIndexJobRepository implements RagIndexJobRepository {

    private final RagIndexJobMapper jobMapper;
    private final RagNestedTransactionExecutor nestedTransactionExecutor;

    public MyBatisFlexRagIndexJobRepository(
            RagIndexJobMapper jobMapper,
            RagNestedTransactionExecutor nestedTransactionExecutor
    ) {
        this.jobMapper = jobMapper;
        this.nestedTransactionExecutor = nestedTransactionExecutor;
    }

    @Override
    public void queue(Long documentId, String contentSha256) {
        nestedTransactionExecutor.executeWithIntegrityRetry(() -> queueOnce(documentId, contentSha256));
    }

    private void queueOnce(Long documentId, String contentSha256) {
        boolean updated = UpdateChain.of(jobMapper)
                .set("status", RagIndexJobStatus.QUEUED.name(), true)
                .set("stage", RagIndexStage.QUEUED.name(), true)
                .set("attempt_count", 0, true)
                .setRaw("target_generation", "NULL", true)
                .setRaw("message_id", "NULL", true)
                .setRaw("last_error", "NULL", true)
                .setRaw("started_at", "NULL", true)
                .setRaw("finished_at", "NULL", true)
                .setRaw("updated_at", "now()", true)
                .where("document_id = ? AND content_sha256 = ?", documentId, contentSha256)
                .update();
        if (updated) {
            return;
        }
        DbChain.table("rag_index_jobs")
                .set("document_id", documentId)
                .set("content_sha256", contentSha256)
                .set("status", RagIndexJobStatus.QUEUED.name())
                .set("stage", RagIndexStage.QUEUED.name())
                .save();
    }

    @Override
    public void attachMessageId(Long documentId, String contentSha256, String messageId) {
        nestedTransactionExecutor.executeWithIntegrityRetry(
                () -> attachMessageIdOnce(documentId, contentSha256, messageId)
        );
    }

    private void attachMessageIdOnce(Long documentId, String contentSha256, String messageId) {
        boolean updated = UpdateChain.of(jobMapper)
                .set("message_id", messageId, true)
                .setRaw("updated_at", "now()", true)
                .where("document_id = ? AND content_sha256 = ?", documentId, contentSha256)
                .update();
        if (updated) {
            return;
        }
        DbChain.table("rag_index_jobs")
                .set("document_id", documentId)
                .set("content_sha256", contentSha256)
                .set("status", RagIndexJobStatus.QUEUED.name())
                .set("stage", RagIndexStage.QUEUED.name())
                .set("message_id", messageId)
                .save();
    }

    @Override
    public void annotateEvent(Long documentId, String contentSha256, String event) {
        UpdateChain.of(jobMapper)
                .set("last_event", event, true)
                .setRaw("version", "version + 1", true)
                .setRaw("updated_at", "now()", true)
                .where("document_id = ? AND content_sha256 = ?", documentId, contentSha256)
                .update();
    }

    @Override
    public int startAttempt(Long documentId, String contentSha256, RagIndexStage fromStage, Long targetGeneration) {
        return updatedRows(UpdateChain.of(jobMapper)
                .set("status", RagIndexJobStatus.RUNNING.name(), true)
                .set("stage", RagIndexStage.PREPARING.name(), true)
                .set("target_generation", targetGeneration, true)
                .setRaw("attempt_count", "attempt_count + 1", true)
                .setRaw("last_error", "NULL", true)
                .setRaw("started_at", "COALESCE(started_at, now())", true)
                .setRaw("finished_at", "NULL", true)
                .setRaw("updated_at", "now()", true)
                .where("document_id = ? AND content_sha256 = ? AND stage = ?",
                        documentId, contentSha256, fromStage.name()));
    }

    @Override
    public int updateStage(Long documentId, String contentSha256, RagIndexStage fromStage, RagIndexStage toStage) {
        return updatedRows(UpdateChain.of(jobMapper)
                .set("stage", toStage.name(), true)
                .setRaw("updated_at", "now()", true)
                .where("document_id = ? AND content_sha256 = ? AND stage = ?",
                        documentId, contentSha256, fromStage.name()));
    }

    @Override
    public void recordRetry(Long documentId, String contentSha256, RagIndexStage stage, String errorMessage) {
        UpdateChain.of(jobMapper)
                .set("status", RagIndexJobStatus.QUEUED.name(), true)
                .set("stage", stage.name(), true)
                .set("last_error", errorMessage, true)
                .setRaw("updated_at", "now()", true)
                .where("document_id = ? AND content_sha256 = ?", documentId, contentSha256)
                .update();
    }

    @Override
    public int markSucceeded(Long documentId, String contentSha256, RagIndexStage fromStage, Long targetGeneration) {
        return updatedRows(UpdateChain.of(jobMapper)
                .set("status", RagIndexJobStatus.SUCCEEDED.name(), true)
                .set("stage", RagIndexStage.COMPLETED.name(), true)
                .set("target_generation", targetGeneration, true)
                .setRaw("last_error", "NULL", true)
                .setRaw("finished_at", "now()", true)
                .setRaw("updated_at", "now()", true)
                .where("document_id = ? AND content_sha256 = ? AND stage = ?",
                        documentId, contentSha256, fromStage.name()));
    }

    @Override
    public int markFailed(
            Long documentId,
            String contentSha256,
            RagIndexStage fromStage,
            RagIndexStage failureStage,
            String errorMessage
    ) {
        return updatedRows(UpdateChain.of(jobMapper)
                .set("status", RagIndexJobStatus.FAILED.name(), true)
                .set("stage", failureStage.name(), true)
                .set("last_error", errorMessage, true)
                .setRaw("finished_at", "now()", true)
                .setRaw("updated_at", "now()", true)
                .where("document_id = ? AND content_sha256 = ? AND stage = ?",
                        documentId, contentSha256, fromStage.name()));
    }

    @Override
    public int markSkipped(Long documentId, String contentSha256, RagIndexStage fromStage, String reason) {
        return updatedRows(UpdateChain.of(jobMapper)
                .set("status", RagIndexJobStatus.SKIPPED.name(), true)
                .set("stage", RagIndexStage.SKIPPED.name(), true)
                .set("last_error", reason, true)
                .setRaw("finished_at", "now()", true)
                .setRaw("updated_at", "now()", true)
                .where("document_id = ? AND content_sha256 = ? AND stage = ?",
                        documentId, contentSha256, fromStage.name()));
    }

    @Override
    public Optional<RagIndexJobRecord> findByDocumentIdAndContentSha256(Long documentId, String contentSha256) {
        return QueryChain.of(jobMapper)
                .where("document_id = ? AND content_sha256 = ?", documentId, contentSha256)
                .limit(1)
                .oneOpt()
                .map(this::toRecord);
    }

    @Override
    public void deleteByDocumentId(Long documentId) {
        DbChain.table("rag_index_jobs").where("document_id = ?", documentId).remove();
    }

    @Override
    public List<RagIndexJobRecord> findStaleJobs(OffsetDateTime updatedBefore, int limit) {
        return QueryChain.of(jobMapper)
                .where("status NOT IN (?, ?, ?)",
                        RagIndexJobStatus.SUCCEEDED.name(),
                        RagIndexJobStatus.FAILED.name(),
                        RagIndexJobStatus.SKIPPED.name())
                .and("updated_at < ?", Timestamp.from(updatedBefore.toInstant()))
                .orderBy("updated_at")
                .limit(limit)
                .list()
                .stream()
                .map(this::toRecord)
                .toList();
    }

    private int updatedRows(UpdateChain<RagIndexJobEntity> update) {
        return update.update() ? 1 : 0;
    }

    private RagIndexJobRecord toRecord(RagIndexJobEntity entity) {
        return new RagIndexJobRecord(
                entity.getId(),
                entity.getDocumentId(),
                entity.getContentSha256(),
                entity.getStatus(),
                entity.getStage(),
                entity.getVersion() == null ? 0L : entity.getVersion(),
                entity.getLastEvent(),
                entity.getAttemptCount() == null ? 0 : entity.getAttemptCount(),
                entity.getTargetGeneration(),
                entity.getMessageId(),
                entity.getLastError(),
                entity.getStartedAt(),
                entity.getFinishedAt(),
                entity.getCreatedAt(),
                entity.getUpdatedAt()
        );
    }
}
