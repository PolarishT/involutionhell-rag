package com.involutionhell.backend.rag.indexing.persistence.jdbc;

import com.involutionhell.backend.rag.indexing.model.RagIndexOutboxEventType;
import com.involutionhell.backend.rag.indexing.model.RagIndexOutboxStatus;
import com.involutionhell.backend.rag.indexing.persistence.RagIndexOutboxRecord;
import com.involutionhell.backend.rag.indexing.persistence.RagIndexOutboxRepository;
import com.involutionhell.backend.rag.shared.persistence.RagJdbcRows;
import com.involutionhell.backend.rag.shared.persistence.RagNestedTransactionExecutor;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
public class JdbcRagIndexOutboxRepository implements RagIndexOutboxRepository {

    private static final String SELECT_COLUMNS = """
            id, document_id, content_sha256, event_type, status, attempt_count,
            message_id, last_error, next_attempt_at, dispatched_at, consumed_at,
            created_at, updated_at
            """;

    private final JdbcClient jdbcClient;
    private final RagNestedTransactionExecutor nestedTransactionExecutor;

    public JdbcRagIndexOutboxRepository(
            JdbcClient jdbcClient,
            RagNestedTransactionExecutor nestedTransactionExecutor
    ) {
        this.jdbcClient = jdbcClient;
        this.nestedTransactionExecutor = nestedTransactionExecutor;
    }

    @Override
    public void enqueue(Long documentId, String contentSha256, RagIndexOutboxEventType eventType) {
        nestedTransactionExecutor.executeWithIntegrityRetry(() -> enqueueOnce(documentId, contentSha256, eventType));
    }

    private void enqueueOnce(Long documentId, String contentSha256, RagIndexOutboxEventType eventType) {
        int updated = jdbcClient.sql("""
                UPDATE rag_index_outbox
                SET status = :status,
                    attempt_count = 0,
                    message_id = NULL,
                    last_error = NULL,
                    next_attempt_at = now(),
                    dispatched_at = NULL,
                    consumed_at = NULL,
                    updated_at = now()
                WHERE document_id = :documentId
                  AND content_sha256 = :contentSha256
                  AND event_type = :eventType
                """)
                .param("status", RagIndexOutboxStatus.NEW.name())
                .param("documentId", documentId)
                .param("contentSha256", contentSha256)
                .param("eventType", eventType.name())
                .update();
        if (updated > 0) {
            return;
        }
        jdbcClient.sql("""
                INSERT INTO rag_index_outbox (
                    document_id, content_sha256, event_type, status, next_attempt_at
                ) VALUES (
                    :documentId, :contentSha256, :eventType, :status, now()
                )
                """)
                .param("documentId", documentId)
                .param("contentSha256", contentSha256)
                .param("eventType", eventType.name())
                .param("status", RagIndexOutboxStatus.NEW.name())
                .update();
    }

    @Override
    public List<RagIndexOutboxRecord> findDispatchable(OffsetDateTime now, int limit) {
        return jdbcClient.sql("SELECT " + SELECT_COLUMNS + """
                 FROM rag_index_outbox
                 WHERE status IN (:newStatus, :failedStatus)
                   AND (next_attempt_at IS NULL OR next_attempt_at <= :now)
                 ORDER BY created_at ASC
                 LIMIT :limit
                """)
                .param("newStatus", RagIndexOutboxStatus.NEW.name())
                .param("failedStatus", RagIndexOutboxStatus.FAILED.name())
                .param("now", Timestamp.from(now.toInstant()))
                .param("limit", limit)
                .query(this::mapOutbox)
                .list();
    }

    @Override
    public boolean markSending(Long id) {
        return jdbcClient.sql("""
                UPDATE rag_index_outbox
                SET status = :sending, updated_at = now()
                WHERE id = :id AND status IN (:newStatus, :failedStatus)
                """)
                .param("sending", RagIndexOutboxStatus.SENDING.name())
                .param("id", id)
                .param("newStatus", RagIndexOutboxStatus.NEW.name())
                .param("failedStatus", RagIndexOutboxStatus.FAILED.name())
                .update() > 0;
    }

    @Override
    public void markSent(Long id, String messageId) {
        jdbcClient.sql("""
                UPDATE rag_index_outbox
                SET status = :status,
                    message_id = :messageId,
                    last_error = NULL,
                    dispatched_at = now(),
                    updated_at = now()
                WHERE id = :id
                """)
                .param("status", RagIndexOutboxStatus.SENT.name())
                .param("messageId", messageId)
                .param("id", id)
                .update();
    }

    @Override
    public boolean confirmConsumed(Long documentId, String contentSha256, String messageId) {
        return jdbcClient.sql("""
                UPDATE rag_index_outbox
                SET status = :status,
                    message_id = COALESCE(message_id, :messageId),
                    last_error = NULL,
                    dispatched_at = COALESCE(dispatched_at, now()),
                    consumed_at = COALESCE(consumed_at, now()),
                    updated_at = now()
                WHERE document_id = :documentId
                  AND content_sha256 = :contentSha256
                  AND event_type = :eventType
                  AND (message_id IS NULL OR message_id = :messageId)
                """)
                .param("status", RagIndexOutboxStatus.SENT.name())
                .param("messageId", messageId)
                .param("documentId", documentId)
                .param("contentSha256", contentSha256)
                .param("eventType", RagIndexOutboxEventType.INDEX_DOCUMENT.name())
                .update() > 0;
    }

    @Override
    public boolean confirmConsumedByMessageId(String messageId) {
        return jdbcClient.sql("""
                UPDATE rag_index_outbox
                SET status = :status,
                    last_error = NULL,
                    dispatched_at = COALESCE(dispatched_at, now()),
                    consumed_at = COALESCE(consumed_at, now()),
                    updated_at = now()
                WHERE message_id = :messageId
                """)
                .param("status", RagIndexOutboxStatus.SENT.name())
                .param("messageId", messageId)
                .update() > 0;
    }

    @Override
    public void markFailed(Long id, String errorMessage, OffsetDateTime nextAttemptAt) {
        jdbcClient.sql("""
                UPDATE rag_index_outbox
                SET status = :status,
                    attempt_count = attempt_count + 1,
                    last_error = :lastError,
                    next_attempt_at = :nextAttemptAt,
                    updated_at = now()
                WHERE id = :id
                """)
                .param("status", RagIndexOutboxStatus.FAILED.name())
                .param("lastError", errorMessage)
                .param("nextAttemptAt", Timestamp.from(nextAttemptAt.toInstant()))
                .param("id", id)
                .update();
    }

    @Override
    public List<RagIndexOutboxRecord> findStuckSendingBefore(OffsetDateTime cutoff, int limit) {
        return jdbcClient.sql("SELECT " + SELECT_COLUMNS + """
                 FROM rag_index_outbox
                 WHERE status = :status AND updated_at <= :cutoff
                 ORDER BY updated_at ASC
                 LIMIT :limit
                """)
                .param("status", RagIndexOutboxStatus.SENDING.name())
                .param("cutoff", Timestamp.from(cutoff.toInstant()))
                .param("limit", limit)
                .query(this::mapOutbox)
                .list();
    }

    @Override
    public void resetForRetry(Long id, String errorMessage, OffsetDateTime nextAttemptAt) {
        jdbcClient.sql("""
                UPDATE rag_index_outbox
                SET status = :status,
                    last_error = :lastError,
                    next_attempt_at = :nextAttemptAt,
                    updated_at = now()
                WHERE id = :id
                """)
                .param("status", RagIndexOutboxStatus.FAILED.name())
                .param("lastError", errorMessage)
                .param("nextAttemptAt", Timestamp.from(nextAttemptAt.toInstant()))
                .param("id", id)
                .update();
    }

    @Override
    public Optional<RagIndexOutboxRecord> findByDocumentIdAndContentSha256(
            Long documentId,
            String contentSha256
    ) {
        return jdbcClient.sql("SELECT " + SELECT_COLUMNS + """
                 FROM rag_index_outbox
                 WHERE document_id = :documentId AND content_sha256 = :contentSha256
                 ORDER BY updated_at DESC
                 LIMIT 1
                """)
                .param("documentId", documentId)
                .param("contentSha256", contentSha256)
                .query(this::mapOutbox)
                .optional();
    }

    @Override
    public void deleteByDocumentId(Long documentId) {
        jdbcClient.sql("DELETE FROM rag_index_outbox WHERE document_id = :documentId")
                .param("documentId", documentId)
                .update();
    }

    private RagIndexOutboxRecord mapOutbox(ResultSet resultSet, int rowNumber) throws SQLException {
        Integer attemptCount = RagJdbcRows.intValue(resultSet, "attempt_count");
        return new RagIndexOutboxRecord(
                RagJdbcRows.longValue(resultSet, "id"),
                RagJdbcRows.longValue(resultSet, "document_id"),
                RagJdbcRows.string(resultSet, "content_sha256"),
                RagJdbcRows.string(resultSet, "event_type"),
                RagJdbcRows.string(resultSet, "status"),
                attemptCount == null ? 0 : attemptCount,
                RagJdbcRows.string(resultSet, "message_id"),
                RagJdbcRows.string(resultSet, "last_error"),
                RagJdbcRows.offsetDateTime(resultSet, "next_attempt_at"),
                RagJdbcRows.offsetDateTime(resultSet, "dispatched_at"),
                RagJdbcRows.offsetDateTime(resultSet, "consumed_at"),
                RagJdbcRows.offsetDateTime(resultSet, "created_at"),
                RagJdbcRows.offsetDateTime(resultSet, "updated_at")
        );
    }
}
