package com.involutionhell.backend.rag.indexing.persistence.mybatis;

import com.involutionhell.backend.rag.indexing.persistence.RagIndexJobTransitionRecord;
import com.involutionhell.backend.rag.indexing.persistence.RagIndexJobTransitionRepository;
import com.involutionhell.backend.rag.shared.persistence.RagDatabaseDialect;
import com.involutionhell.backend.rag.shared.persistence.RagFlexJson;
import com.involutionhell.backend.rag.shared.support.RagJsonCodec;
import com.mybatisflex.core.query.QueryChain;
import com.mybatisflex.core.row.DbChain;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Repository;

@Repository
public class MyBatisFlexRagIndexJobTransitionRepository implements RagIndexJobTransitionRepository {

    private final RagIndexJobTransitionMapper transitionMapper;
    private final RagJsonCodec jsonCodec;
    private final RagDatabaseDialect databaseDialect;

    public MyBatisFlexRagIndexJobTransitionRepository(
            RagIndexJobTransitionMapper transitionMapper,
            RagJsonCodec jsonCodec,
            RagDatabaseDialect databaseDialect
    ) {
        this.transitionMapper = transitionMapper;
        this.jsonCodec = jsonCodec;
        this.databaseDialect = databaseDialect;
    }

    @Override
    public void save(
            Long documentId,
            Long jobId,
            Long outboxId,
            String contentSha256,
            String fromState,
            String toState,
            String event,
            String triggerType,
            String triggeredBy,
            boolean success,
            String failureReason,
            String errorMessage,
            String messageId,
            Map<String, Object> metadata
    ) {
        DbChain insert = DbChain.table("rag_index_job_transitions")
                .set("document_id", documentId)
                .set("job_id", jobId)
                .set("outbox_id", outboxId)
                .set("content_sha256", contentSha256)
                .set("from_state", fromState)
                .set("to_state", toState)
                .set("event", event)
                .set("trigger_type", triggerType)
                .set("triggered_by", triggeredBy)
                .set("success", success)
                .set("failure_reason", failureReason)
                .set("error_message", errorMessage)
                .set("message_id", messageId);
        RagFlexJson.setJson(insert, databaseDialect, "metadata", jsonCodec.write(metadata == null ? Map.of() : metadata))
                .save();
    }

    @Override
    public List<RagIndexJobTransitionRecord> findByDocumentIdAndContentSha256(Long documentId, String contentSha256) {
        return QueryChain.of(transitionMapper)
                .where("document_id = ? AND content_sha256 = ?", documentId, contentSha256)
                .orderBy("created_at ASC", "id ASC")
                .list()
                .stream()
                .map(this::toRecord)
                .toList();
    }

    @Override
    public void deleteByDocumentId(Long documentId) {
        DbChain.table("rag_index_job_transitions").where("document_id = ?", documentId).remove();
    }

    private RagIndexJobTransitionRecord toRecord(RagIndexJobTransitionEntity entity) {
        return new RagIndexJobTransitionRecord(
                entity.getId(),
                entity.getDocumentId(),
                entity.getJobId(),
                entity.getOutboxId(),
                entity.getContentSha256(),
                entity.getFromState(),
                entity.getToState(),
                entity.getEvent(),
                entity.getTriggerType(),
                entity.getTriggeredBy(),
                Boolean.TRUE.equals(entity.getSuccess()),
                entity.getFailureReason(),
                entity.getErrorMessage(),
                entity.getMessageId(),
                jsonCodec.readMap(entity.getMetadata()),
                entity.getCreatedAt()
        );
    }
}
