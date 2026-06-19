package com.involutionhell.backend.rag.indexing.persistence.jdbc;

import com.involutionhell.backend.rag.indexing.model.RagIndexJobStatus;
import com.involutionhell.backend.rag.indexing.model.RagIndexStage;
import com.involutionhell.backend.rag.indexing.persistence.RagIndexJobRecord;
import com.involutionhell.backend.rag.indexing.persistence.RagIndexJobRepository;
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
public class JdbcRagIndexJobRepository implements RagIndexJobRepository {

    private static final String SELECT_COLUMNS = """
            id, document_id, content_sha256, status, stage, version, last_event,
            attempt_count, target_generation, message_id, last_error, started_at,
            finished_at, created_at, updated_at
            """;

    private final JdbcClient jdbcClient;
    private final RagNestedTransactionExecutor nestedTransactionExecutor;

    public JdbcRagIndexJobRepository(
            JdbcClient jdbcClient,
            RagNestedTransactionExecutor nestedTransactionExecutor
    ) {
        this.jdbcClient = jdbcClient;
        this.nestedTransactionExecutor = nestedTransactionExecutor;
    }

    @Override
    public void queue(Long documentId, String contentSha256) {
        nestedTransactionExecutor.executeWithIntegrityRetry(() -> queueOnce(documentId, contentSha256));
    }

    private void queueOnce(Long documentId, String contentSha256) {
        int updated = jdbcClient.sql("""
                UPDATE rag_index_jobs
                SET status = :status,
                    stage = :stage,
                    attempt_count = 0,
                    target_generation = NULL,
                    message_id = NULL,
                    last_error = NULL,
                    started_at = NULL,
                    finished_at = NULL,
                    updated_at = now()
                WHERE document_id = :documentId AND content_sha256 = :contentSha256
                """)
                .param("status", RagIndexJobStatus.QUEUED.name())
                .param("stage", RagIndexStage.QUEUED.name())
                .param("documentId", documentId)
                .param("contentSha256", contentSha256)
                .update();
        if (updated > 0) {
            return;
        }
        insertQueued(documentId, contentSha256, null);
    }

    @Override
    public void attachMessageId(Long documentId, String contentSha256, String messageId) {
        nestedTransactionExecutor.executeWithIntegrityRetry(
                () -> attachMessageIdOnce(documentId, contentSha256, messageId)
        );
    }

    private void attachMessageIdOnce(Long documentId, String contentSha256, String messageId) {
        int updated = jdbcClient.sql("""
                UPDATE rag_index_jobs
                SET message_id = :messageId, updated_at = now()
                WHERE document_id = :documentId AND content_sha256 = :contentSha256
                """)
                .param("messageId", messageId)
                .param("documentId", documentId)
                .param("contentSha256", contentSha256)
                .update();
        if (updated > 0) {
            return;
        }
        insertQueued(documentId, contentSha256, messageId);
    }

    @Override
    public void annotateEvent(Long documentId, String contentSha256, String event) {
        jdbcClient.sql("""
                UPDATE rag_index_jobs
                SET last_event = :event, version = version + 1, updated_at = now()
                WHERE document_id = :documentId AND content_sha256 = :contentSha256
                """)
                .param("event", event)
                .param("documentId", documentId)
                .param("contentSha256", contentSha256)
                .update();
    }

    @Override
    public int startAttempt(Long documentId, String contentSha256, RagIndexStage fromStage, Long targetGeneration) {
        return jdbcClient.sql("""
                UPDATE rag_index_jobs
                SET status = :status,
                    stage = :toStage,
                    target_generation = :targetGeneration,
                    attempt_count = attempt_count + 1,
                    last_error = NULL,
                    started_at = COALESCE(started_at, now()),
                    finished_at = NULL,
                    updated_at = now()
                WHERE document_id = :documentId
                  AND content_sha256 = :contentSha256
                  AND stage = :fromStage
                """)
                .param("status", RagIndexJobStatus.RUNNING.name())
                .param("toStage", RagIndexStage.PREPARING.name())
                .param("targetGeneration", targetGeneration)
                .param("documentId", documentId)
                .param("contentSha256", contentSha256)
                .param("fromStage", fromStage.name())
                .update();
    }

    @Override
    public int updateStage(Long documentId, String contentSha256, RagIndexStage fromStage, RagIndexStage toStage) {
        return jdbcClient.sql("""
                UPDATE rag_index_jobs
                SET stage = :toStage, updated_at = now()
                WHERE document_id = :documentId
                  AND content_sha256 = :contentSha256
                  AND stage = :fromStage
                """)
                .param("toStage", toStage.name())
                .param("documentId", documentId)
                .param("contentSha256", contentSha256)
                .param("fromStage", fromStage.name())
                .update();
    }

    @Override
    public void recordRetry(Long documentId, String contentSha256, RagIndexStage stage, String errorMessage) {
        jdbcClient.sql("""
                UPDATE rag_index_jobs
                SET status = :status, stage = :stage, last_error = :lastError, updated_at = now()
                WHERE document_id = :documentId AND content_sha256 = :contentSha256
                """)
                .param("status", RagIndexJobStatus.QUEUED.name())
                .param("stage", stage.name())
                .param("lastError", errorMessage)
                .param("documentId", documentId)
                .param("contentSha256", contentSha256)
                .update();
    }

    @Override
    public int markSucceeded(Long documentId, String contentSha256, RagIndexStage fromStage, Long targetGeneration) {
        return finish(
                documentId,
                contentSha256,
                fromStage,
                RagIndexJobStatus.SUCCEEDED,
                RagIndexStage.COMPLETED,
                targetGeneration,
                null
        );
    }

    @Override
    public int markFailed(
            Long documentId,
            String contentSha256,
            RagIndexStage fromStage,
            RagIndexStage failureStage,
            String errorMessage
    ) {
        return finish(
                documentId,
                contentSha256,
                fromStage,
                RagIndexJobStatus.FAILED,
                failureStage,
                null,
                errorMessage
        );
    }

    @Override
    public int markSkipped(Long documentId, String contentSha256, RagIndexStage fromStage, String reason) {
        return finish(
                documentId,
                contentSha256,
                fromStage,
                RagIndexJobStatus.SKIPPED,
                RagIndexStage.SKIPPED,
                null,
                reason
        );
    }

    @Override
    public Optional<RagIndexJobRecord> findByDocumentIdAndContentSha256(Long documentId, String contentSha256) {
        return jdbcClient.sql("SELECT " + SELECT_COLUMNS + """
                 FROM rag_index_jobs
                 WHERE document_id = :documentId AND content_sha256 = :contentSha256
                 LIMIT 1
                """)
                .param("documentId", documentId)
                .param("contentSha256", contentSha256)
                .query(this::mapJob)
                .optional();
    }

    @Override
    public void deleteByDocumentId(Long documentId) {
        jdbcClient.sql("DELETE FROM rag_index_jobs WHERE document_id = :documentId")
                .param("documentId", documentId)
                .update();
    }

    @Override
    public List<RagIndexJobRecord> findStaleJobs(OffsetDateTime updatedBefore, int limit) {
        return jdbcClient.sql("SELECT " + SELECT_COLUMNS + """
                 FROM rag_index_jobs
                 WHERE status NOT IN (:succeeded, :failed, :skipped)
                   AND updated_at < :updatedBefore
                 ORDER BY updated_at
                 LIMIT :limit
                """)
                .param("succeeded", RagIndexJobStatus.SUCCEEDED.name())
                .param("failed", RagIndexJobStatus.FAILED.name())
                .param("skipped", RagIndexJobStatus.SKIPPED.name())
                .param("updatedBefore", Timestamp.from(updatedBefore.toInstant()))
                .param("limit", limit)
                .query(this::mapJob)
                .list();
    }

    private void insertQueued(Long documentId, String contentSha256, String messageId) {
        jdbcClient.sql("""
                INSERT INTO rag_index_jobs (
                    document_id, content_sha256, status, stage, message_id
                ) VALUES (
                    :documentId, :contentSha256, :status, :stage, :messageId
                )
                """)
                .param("documentId", documentId)
                .param("contentSha256", contentSha256)
                .param("status", RagIndexJobStatus.QUEUED.name())
                .param("stage", RagIndexStage.QUEUED.name())
                .param("messageId", messageId)
                .update();
    }

    private int finish(
            Long documentId,
            String contentSha256,
            RagIndexStage fromStage,
            RagIndexJobStatus status,
            RagIndexStage toStage,
            Long targetGeneration,
            String errorMessage
    ) {
        return jdbcClient.sql("""
                UPDATE rag_index_jobs
                SET status = :status,
                    stage = :toStage,
                    target_generation = COALESCE(:targetGeneration, target_generation),
                    last_error = :lastError,
                    finished_at = now(),
                    updated_at = now()
                WHERE document_id = :documentId
                  AND content_sha256 = :contentSha256
                  AND stage = :fromStage
                """)
                .param("status", status.name())
                .param("toStage", toStage.name())
                .param("targetGeneration", targetGeneration)
                .param("lastError", errorMessage)
                .param("documentId", documentId)
                .param("contentSha256", contentSha256)
                .param("fromStage", fromStage.name())
                .update();
    }

    private RagIndexJobRecord mapJob(ResultSet resultSet, int rowNumber) throws SQLException {
        Long version = RagJdbcRows.longValue(resultSet, "version");
        Integer attemptCount = RagJdbcRows.intValue(resultSet, "attempt_count");
        return new RagIndexJobRecord(
                RagJdbcRows.longValue(resultSet, "id"),
                RagJdbcRows.longValue(resultSet, "document_id"),
                RagJdbcRows.string(resultSet, "content_sha256"),
                RagJdbcRows.string(resultSet, "status"),
                RagJdbcRows.string(resultSet, "stage"),
                version == null ? 0L : version,
                RagJdbcRows.string(resultSet, "last_event"),
                attemptCount == null ? 0 : attemptCount,
                RagJdbcRows.longValue(resultSet, "target_generation"),
                RagJdbcRows.string(resultSet, "message_id"),
                RagJdbcRows.string(resultSet, "last_error"),
                RagJdbcRows.offsetDateTime(resultSet, "started_at"),
                RagJdbcRows.offsetDateTime(resultSet, "finished_at"),
                RagJdbcRows.offsetDateTime(resultSet, "created_at"),
                RagJdbcRows.offsetDateTime(resultSet, "updated_at")
        );
    }
}
