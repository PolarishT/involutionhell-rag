package com.involutionhell.backend.rag.indexing.persistence.mybatis;

import com.involutionhell.backend.rag.indexing.model.RagIndexOutboxEventType;
import com.involutionhell.backend.rag.indexing.model.RagIndexOutboxStatus;
import com.involutionhell.backend.rag.indexing.persistence.RagIndexOutboxRecord;
import com.involutionhell.backend.rag.indexing.persistence.RagIndexOutboxRepository;
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
public class MyBatisFlexRagIndexOutboxRepository implements RagIndexOutboxRepository {

    private final RagIndexOutboxMapper outboxMapper;
    private final RagNestedTransactionExecutor nestedTransactionExecutor;

    public MyBatisFlexRagIndexOutboxRepository(
            RagIndexOutboxMapper outboxMapper,
            RagNestedTransactionExecutor nestedTransactionExecutor
    ) {
        this.outboxMapper = outboxMapper;
        this.nestedTransactionExecutor = nestedTransactionExecutor;
    }

    @Override
    public void enqueue(Long documentId, String contentSha256, RagIndexOutboxEventType eventType) {
        nestedTransactionExecutor.executeWithIntegrityRetry(() -> enqueueOnce(documentId, contentSha256, eventType));
    }

    private void enqueueOnce(Long documentId, String contentSha256, RagIndexOutboxEventType eventType) {
        boolean updated = UpdateChain.of(outboxMapper)
                .set("status", RagIndexOutboxStatus.NEW.name(), true)
                .set("attempt_count", 0, true)
                .setRaw("message_id", "NULL", true)
                .setRaw("last_error", "NULL", true)
                .setRaw("next_attempt_at", "now()", true)
                .setRaw("dispatched_at", "NULL", true)
                .setRaw("consumed_at", "NULL", true)
                .setRaw("updated_at", "now()", true)
                .where("document_id = ? AND content_sha256 = ? AND event_type = ?",
                        documentId, contentSha256, eventType.name())
                .update();
        if (updated) {
            return;
        }
        DbChain.table("rag_index_outbox")
                .set("document_id", documentId)
                .set("content_sha256", contentSha256)
                .set("event_type", eventType.name())
                .set("status", RagIndexOutboxStatus.NEW.name())
                .setRaw("next_attempt_at", "now()")
                .save();
    }

    @Override
    public List<RagIndexOutboxRecord> findDispatchable(OffsetDateTime now, int limit) {
        return QueryChain.of(outboxMapper)
                .where("status IN (?, ?)", RagIndexOutboxStatus.NEW.name(), RagIndexOutboxStatus.FAILED.name())
                .and("(next_attempt_at IS NULL OR next_attempt_at <= ?)", Timestamp.from(now.toInstant()))
                .orderBy("created_at ASC")
                .limit(limit)
                .list()
                .stream()
                .map(this::toRecord)
                .toList();
    }

    @Override
    public boolean markSending(Long id) {
        return UpdateChain.of(outboxMapper)
                .set("status", RagIndexOutboxStatus.SENDING.name(), true)
                .setRaw("updated_at", "now()", true)
                .where("id = ? AND status IN (?, ?)",
                        id, RagIndexOutboxStatus.NEW.name(), RagIndexOutboxStatus.FAILED.name())
                .update();
    }

    @Override
    public void markSent(Long id, String messageId) {
        UpdateChain.of(outboxMapper)
                .set("status", RagIndexOutboxStatus.SENT.name(), true)
                .set("message_id", messageId, true)
                .setRaw("last_error", "NULL", true)
                .setRaw("dispatched_at", "now()", true)
                .setRaw("updated_at", "now()", true)
                .where("id = ?", id)
                .update();
    }

    @Override
    public boolean confirmConsumed(Long documentId, String contentSha256, String messageId) {
        return UpdateChain.of(outboxMapper)
                .set("status", RagIndexOutboxStatus.SENT.name(), true)
                .setRaw("message_id", "COALESCE(message_id, " + sqlLiteral(messageId) + ")", true)
                .setRaw("last_error", "NULL", true)
                .setRaw("dispatched_at", "COALESCE(dispatched_at, now())", true)
                .setRaw("consumed_at", "COALESCE(consumed_at, now())", true)
                .setRaw("updated_at", "now()", true)
                .where("document_id = ? AND content_sha256 = ? AND event_type = ?",
                        documentId, contentSha256, RagIndexOutboxEventType.INDEX_DOCUMENT.name())
                .and("(message_id IS NULL OR message_id = ?)", messageId)
                .update();
    }

    @Override
    public boolean confirmConsumedByMessageId(String messageId) {
        return UpdateChain.of(outboxMapper)
                .set("status", RagIndexOutboxStatus.SENT.name(), true)
                .setRaw("last_error", "NULL", true)
                .setRaw("dispatched_at", "COALESCE(dispatched_at, now())", true)
                .setRaw("consumed_at", "COALESCE(consumed_at, now())", true)
                .setRaw("updated_at", "now()", true)
                .where("message_id = ?", messageId)
                .update();
    }

    @Override
    public void markFailed(Long id, String errorMessage, OffsetDateTime nextAttemptAt) {
        UpdateChain.of(outboxMapper)
                .set("status", RagIndexOutboxStatus.FAILED.name(), true)
                .setRaw("attempt_count", "attempt_count + 1", true)
                .set("last_error", errorMessage, true)
                .set("next_attempt_at", nextAttemptAt, true)
                .setRaw("updated_at", "now()", true)
                .where("id = ?", id)
                .update();
    }

    @Override
    public List<RagIndexOutboxRecord> findStuckSendingBefore(OffsetDateTime cutoff, int limit) {
        return QueryChain.of(outboxMapper)
                .where("status = ?", RagIndexOutboxStatus.SENDING.name())
                .and("updated_at <= ?", Timestamp.from(cutoff.toInstant()))
                .orderBy("updated_at ASC")
                .limit(limit)
                .list()
                .stream()
                .map(this::toRecord)
                .toList();
    }

    @Override
    public void resetForRetry(Long id, String errorMessage, OffsetDateTime nextAttemptAt) {
        UpdateChain.of(outboxMapper)
                .set("status", RagIndexOutboxStatus.FAILED.name(), true)
                .set("last_error", errorMessage, true)
                .set("next_attempt_at", nextAttemptAt, true)
                .setRaw("updated_at", "now()", true)
                .where("id = ?", id)
                .update();
    }

    @Override
    public Optional<RagIndexOutboxRecord> findByDocumentIdAndContentSha256(Long documentId, String contentSha256) {
        return QueryChain.of(outboxMapper)
                .where("document_id = ? AND content_sha256 = ?", documentId, contentSha256)
                .orderBy("updated_at DESC")
                .limit(1)
                .oneOpt()
                .map(this::toRecord);
    }

    @Override
    public void deleteByDocumentId(Long documentId) {
        DbChain.table("rag_index_outbox").where("document_id = ?", documentId).remove();
    }

    private String sqlLiteral(String value) {
        return value == null ? "NULL" : "'" + value.replace("'", "''") + "'";
    }

    private RagIndexOutboxRecord toRecord(RagIndexOutboxEntity entity) {
        return new RagIndexOutboxRecord(
                entity.getId(),
                entity.getDocumentId(),
                entity.getContentSha256(),
                entity.getEventType(),
                entity.getStatus(),
                entity.getAttemptCount() == null ? 0 : entity.getAttemptCount(),
                entity.getMessageId(),
                entity.getLastError(),
                entity.getNextAttemptAt(),
                entity.getDispatchedAt(),
                entity.getConsumedAt(),
                entity.getCreatedAt(),
                entity.getUpdatedAt()
        );
    }
}
