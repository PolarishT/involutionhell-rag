package com.involutionhell.backend.rag.retrieval.persistence.mybatis;

import com.involutionhell.backend.rag.retrieval.persistence.RagAskRunRecord;
import com.involutionhell.backend.rag.retrieval.persistence.RagAskRunRepository;
import com.involutionhell.backend.rag.retrieval.persistence.RagStaleAskRunRecord;
import com.involutionhell.backend.rag.shared.persistence.RagDatabaseDialect;
import com.involutionhell.backend.rag.shared.persistence.RagFlexJson;
import com.involutionhell.backend.rag.shared.support.RagJsonCodec;
import com.mybatisflex.core.query.QueryChain;
import com.mybatisflex.core.row.DbChain;
import com.mybatisflex.core.update.UpdateChain;
import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.springframework.stereotype.Repository;

@Repository
public class MyBatisFlexRagAskRunRepository implements RagAskRunRepository {

    private final RagAskRunMapper askRunMapper;
    private final RagJsonCodec jsonCodec;
    private final RagDatabaseDialect databaseDialect;

    public MyBatisFlexRagAskRunRepository(
            RagAskRunMapper askRunMapper,
            RagJsonCodec jsonCodec,
            RagDatabaseDialect databaseDialect
    ) {
        this.askRunMapper = askRunMapper;
        this.jsonCodec = jsonCodec;
        this.databaseDialect = databaseDialect;
    }

    @Override
    public void createRunning(
            String runId,
            String correlationId,
            String userId,
            Long conversationId,
            Long userMessageId,
            Long assistantMessageId,
            String requestId,
            String question,
            Integer topK,
            Map<String, Object> filters
    ) {
        DbChain insert = DbChain.table("rag_ask_runs")
                .set("run_id", runId)
                .set("correlation_id", correlationId)
                .set("user_id", userId)
                .set("conversation_id", conversationId)
                .set("user_message_id", userMessageId)
                .set("assistant_message_id", assistantMessageId)
                .set("request_id", requestId)
                .set("question", question)
                .set("top_k", topK)
                .set("status", "RUNNING");
        RagFlexJson.setJson(insert, databaseDialect, "filters", jsonCodec.write(filters == null ? Map.of() : filters))
                .save();
    }

    @Override
    public Optional<RagAskRunRecord> findByRequestId(String userId, Long conversationId, String requestId) {
        return QueryChain.of(askRunMapper)
                .where("user_id = ? AND conversation_id = ? AND request_id = ?", userId, conversationId, requestId)
                .limit(1)
                .oneOpt()
                .map(this::toRecord);
    }

    @Override
    public List<RagStaleAskRunRecord> findStaleRunning(OffsetDateTime startedBefore, int limit) {
        return QueryChain.of(askRunMapper)
                .where("status = ?", "RUNNING")
                .and("started_at < ?", Timestamp.from(startedBefore.toInstant()))
                .orderBy("started_at ASC")
                .limit(limit)
                .list()
                .stream()
                .map(entity -> new RagStaleAskRunRecord(
                        entity.getRunId(),
                        entity.getConversationId(),
                        entity.getAssistantMessageId(),
                        entity.getStartedAt()
                ))
                .toList();
    }

    @Override
    public void markSucceeded(
            String runId,
            Long assistantMessageId,
            String retrievalQuestion,
            List<String> retrievalQueries,
            Object retrievedContexts,
            Object notices,
            boolean generatedByModel,
            boolean degraded
    ) {
        UpdateChain<RagAskRunEntity> update = UpdateChain.of(askRunMapper)
                .set("assistant_message_id", assistantMessageId, true)
                .set("retrieval_question", retrievalQuestion, true)
                .set("generated_by_model", generatedByModel, true)
                .set("degraded", degraded, true)
                .set("status", "SUCCEEDED", true)
                .setRaw("error_code", "NULL", true)
                .setRaw("error_message", "NULL", true)
                .setRaw("completed_at", "now()", true);
        RagFlexJson.setJson(
                update,
                databaseDialect,
                "retrieval_queries",
                jsonCodec.write(retrievalQueries == null ? List.of() : retrievalQueries)
        );
        RagFlexJson.setJson(
                update,
                databaseDialect,
                "retrieved_contexts",
                jsonCodec.write(retrievedContexts == null ? List.of() : retrievedContexts)
        );
        RagFlexJson.setJson(
                update,
                databaseDialect,
                "notices",
                jsonCodec.write(notices == null ? List.of() : notices)
        );
        update.where("run_id = ? AND status = ?", runId, "RUNNING").update();
    }

    @Override
    public void markFailed(String runId, String errorCode, String errorMessage, Object notices) {
        UpdateChain<RagAskRunEntity> update = UpdateChain.of(askRunMapper)
                .set("degraded", true, true)
                .set("status", "FAILED", true)
                .set("error_code", errorCode, true)
                .set("error_message", errorMessage, true)
                .setRaw("completed_at", "now()", true);
        RagFlexJson.setJson(
                update,
                databaseDialect,
                "notices",
                jsonCodec.write(notices == null ? List.of() : notices)
        );
        update.where("run_id = ? AND status = ?", runId, "RUNNING").update();
    }

    private RagAskRunRecord toRecord(RagAskRunEntity entity) {
        return new RagAskRunRecord(
                entity.getId(),
                entity.getRunId(),
                entity.getCorrelationId(),
                entity.getUserId(),
                entity.getConversationId(),
                entity.getUserMessageId(),
                entity.getAssistantMessageId(),
                entity.getRequestId(),
                entity.getQuestion(),
                entity.getStatus(),
                entity.getErrorCode(),
                entity.getErrorMessage()
        );
    }
}
