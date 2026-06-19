package com.involutionhell.backend.rag.indexing.persistence.jdbc;

import com.involutionhell.backend.rag.indexing.persistence.RagIndexJobTransitionRecord;
import com.involutionhell.backend.rag.indexing.persistence.RagIndexJobTransitionRepository;
import com.involutionhell.backend.rag.shared.persistence.RagDatabaseDialect;
import com.involutionhell.backend.rag.shared.persistence.RagJdbcJson;
import com.involutionhell.backend.rag.shared.persistence.RagJdbcRows;
import com.involutionhell.backend.rag.shared.support.RagJsonCodec;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
public class JdbcRagIndexJobTransitionRepository implements RagIndexJobTransitionRepository {

    private static final String SELECT_COLUMNS = """
            id, document_id, job_id, outbox_id, content_sha256, from_state, to_state,
            event, trigger_type, triggered_by, success, failure_reason, error_message,
            message_id, metadata, created_at
            """;

    private final JdbcClient jdbcClient;
    private final RagJsonCodec jsonCodec;
    private final RagDatabaseDialect databaseDialect;

    public JdbcRagIndexJobTransitionRepository(
            JdbcClient jdbcClient,
            RagJsonCodec jsonCodec,
            RagDatabaseDialect databaseDialect
    ) {
        this.jdbcClient = jdbcClient;
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
        String metadataParameter = RagJdbcJson.parameter(databaseDialect, "metadata");
        jdbcClient.sql("""
                INSERT INTO rag_index_job_transitions (
                    document_id, job_id, outbox_id, content_sha256, from_state, to_state,
                    event, trigger_type, triggered_by, success, failure_reason, error_message,
                    message_id, metadata
                ) VALUES (
                    :documentId, :jobId, :outboxId, :contentSha256, :fromState, :toState,
                    :event, :triggerType, :triggeredBy, :success, :failureReason, :errorMessage,
                    :messageId, %s
                )
                """.formatted(metadataParameter))
                .param("documentId", documentId)
                .param("jobId", jobId)
                .param("outboxId", outboxId)
                .param("contentSha256", contentSha256)
                .param("fromState", fromState)
                .param("toState", toState)
                .param("event", event)
                .param("triggerType", triggerType)
                .param("triggeredBy", triggeredBy)
                .param("success", success)
                .param("failureReason", failureReason)
                .param("errorMessage", errorMessage)
                .param("messageId", messageId)
                .param("metadata", jsonCodec.write(metadata == null ? Map.of() : metadata))
                .update();
    }

    @Override
    public List<RagIndexJobTransitionRecord> findByDocumentIdAndContentSha256(
            Long documentId,
            String contentSha256
    ) {
        return jdbcClient.sql("SELECT " + SELECT_COLUMNS + """
                 FROM rag_index_job_transitions
                 WHERE document_id = :documentId AND content_sha256 = :contentSha256
                 ORDER BY created_at ASC, id ASC
                """)
                .param("documentId", documentId)
                .param("contentSha256", contentSha256)
                .query(this::mapTransition)
                .list();
    }

    @Override
    public void deleteByDocumentId(Long documentId) {
        jdbcClient.sql("DELETE FROM rag_index_job_transitions WHERE document_id = :documentId")
                .param("documentId", documentId)
                .update();
    }

    private RagIndexJobTransitionRecord mapTransition(ResultSet resultSet, int rowNumber) throws SQLException {
        return new RagIndexJobTransitionRecord(
                RagJdbcRows.longValue(resultSet, "id"),
                RagJdbcRows.longValue(resultSet, "document_id"),
                RagJdbcRows.longValue(resultSet, "job_id"),
                RagJdbcRows.longValue(resultSet, "outbox_id"),
                RagJdbcRows.string(resultSet, "content_sha256"),
                RagJdbcRows.string(resultSet, "from_state"),
                RagJdbcRows.string(resultSet, "to_state"),
                RagJdbcRows.string(resultSet, "event"),
                RagJdbcRows.string(resultSet, "trigger_type"),
                RagJdbcRows.string(resultSet, "triggered_by"),
                Boolean.TRUE.equals(RagJdbcRows.booleanValue(resultSet, "success")),
                RagJdbcRows.string(resultSet, "failure_reason"),
                RagJdbcRows.string(resultSet, "error_message"),
                RagJdbcRows.string(resultSet, "message_id"),
                RagJdbcRows.jsonMap(jsonCodec, resultSet, "metadata"),
                RagJdbcRows.offsetDateTime(resultSet, "created_at")
        );
    }
}
