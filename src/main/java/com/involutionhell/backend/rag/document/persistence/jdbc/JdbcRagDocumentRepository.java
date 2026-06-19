package com.involutionhell.backend.rag.document.persistence.jdbc;

import com.involutionhell.backend.rag.document.persistence.RagDocumentRecord;
import com.involutionhell.backend.rag.document.persistence.RagDocumentRepository;
import com.involutionhell.backend.rag.shared.model.RagDocumentStatus;
import com.involutionhell.backend.rag.shared.persistence.RagDatabaseDialect;
import com.involutionhell.backend.rag.shared.persistence.RagJdbcJson;
import com.involutionhell.backend.rag.shared.persistence.RagJdbcRows;
import com.involutionhell.backend.rag.shared.support.RagJsonCodec;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
public class JdbcRagDocumentRepository implements RagDocumentRepository {

    private static final String SELECT_COLUMNS = """
            id, source_type, source_uri, external_ref, title, content, content_sha256,
            indexed_generation, status, chunk_count, attempt_count, metadata, last_error,
            last_attempted_at, indexed_at, created_at, updated_at
            """;

    private final JdbcClient jdbcClient;
    private final RagJsonCodec jsonCodec;
    private final RagDatabaseDialect databaseDialect;

    public JdbcRagDocumentRepository(
            JdbcClient jdbcClient,
            RagJsonCodec jsonCodec,
            RagDatabaseDialect databaseDialect
    ) {
        this.jdbcClient = jdbcClient;
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
        String metadataParameter = RagJdbcJson.parameter(databaseDialect, "metadata");
        jdbcClient.sql("""
                INSERT INTO rag_documents (
                    source_type, source_uri, external_ref, title, content, content_sha256, status, metadata
                ) VALUES (
                    :sourceType, :sourceUri, :externalRef, :title, :content, :contentSha256, :status, %s
                )
                """.formatted(metadataParameter))
                .param("sourceType", sourceType)
                .param("sourceUri", sourceUri)
                .param("externalRef", externalRef)
                .param("title", title)
                .param("content", content)
                .param("contentSha256", contentSha256)
                .param("status", RagDocumentStatus.PENDING.name())
                .param("metadata", jsonCodec.write(metadata == null ? Map.of() : metadata))
                .update();
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
        String metadataParameter = RagJdbcJson.parameter(databaseDialect, "metadata");
        int updated = jdbcClient.sql("""
                UPDATE rag_documents
                SET source_type = :sourceType,
                    source_uri = :sourceUri,
                    external_ref = :externalRef,
                    title = :title,
                    content = :content,
                    content_sha256 = :contentSha256,
                    status = :status,
                    attempt_count = 0,
                    metadata = %s,
                    last_error = NULL,
                    last_attempted_at = NULL,
                    updated_at = now()
                WHERE id = :id AND status <> :deleting
                """.formatted(metadataParameter))
                .param("sourceType", sourceType)
                .param("sourceUri", sourceUri)
                .param("externalRef", externalRef)
                .param("title", title)
                .param("content", content)
                .param("contentSha256", contentSha256)
                .param("status", RagDocumentStatus.PENDING.name())
                .param("metadata", jsonCodec.write(metadata == null ? Map.of() : metadata))
                .param("id", id)
                .param("deleting", RagDocumentStatus.DELETING.name())
                .update();
        if (updated == 0) {
            throw new IllegalArgumentException("RAG 文档不存在: " + id);
        }
        return findById(id).orElseThrow(() -> new IllegalStateException("更新 RAG 文档后查询失败"));
    }

    @Override
    public Optional<RagDocumentRecord> findById(Long id) {
        return jdbcClient.sql("SELECT " + SELECT_COLUMNS + " FROM rag_documents WHERE id = :id")
                .param("id", id)
                .query(this::mapDocument)
                .optional();
    }

    @Override
    public List<RagDocumentRecord> findPendingBefore(OffsetDateTime cutoff, int limit) {
        return findByStatusBefore(RagDocumentStatus.PENDING, "updated_at", cutoff, limit);
    }

    @Override
    public List<RagDocumentRecord> findProcessingBefore(OffsetDateTime cutoff, int limit) {
        return findByStatusBefore(
                RagDocumentStatus.PROCESSING,
                "COALESCE(last_attempted_at, updated_at)",
                cutoff,
                limit
        );
    }

    @Override
    public List<RagDocumentRecord> findFailedBefore(OffsetDateTime cutoff, int limit) {
        return findByStatusBefore(RagDocumentStatus.FAILED, "updated_at", cutoff, limit);
    }

    @Override
    public List<RagDocumentRecord> findDeletingBefore(OffsetDateTime cutoff, int limit) {
        return findByStatusBefore(RagDocumentStatus.DELETING, "updated_at", cutoff, limit);
    }

    @Override
    public void markPending(Long id) {
        updateUnlessDeleting(id, """
                UPDATE rag_documents
                SET status = :status,
                    attempt_count = 0,
                    last_error = NULL,
                    last_attempted_at = NULL,
                    updated_at = now()
                WHERE id = :id AND status <> :deleting
                """, parameters("status", RagDocumentStatus.PENDING.name()));
    }

    @Override
    public void requeue(Long id, String note) {
        updateUnlessDeleting(id, """
                UPDATE rag_documents
                SET status = :status, last_error = :note, updated_at = now()
                WHERE id = :id AND status <> :deleting
                """, parameters("status", RagDocumentStatus.PENDING.name(), "note", note));
    }

    @Override
    public void markProcessing(Long id) {
        updateUnlessDeleting(id, """
                UPDATE rag_documents
                SET status = :status,
                    attempt_count = attempt_count + 1,
                    last_error = NULL,
                    last_attempted_at = now(),
                    updated_at = now()
                WHERE id = :id AND status <> :deleting
                """, parameters("status", RagDocumentStatus.PROCESSING.name()));
    }

    @Override
    public void markIndexed(Long id, Long indexedGeneration, int chunkCount, OffsetDateTime indexedAt) {
        updateUnlessDeleting(id, """
                UPDATE rag_documents
                SET status = :status,
                    indexed_generation = :indexedGeneration,
                    chunk_count = :chunkCount,
                    indexed_at = :indexedAt,
                    last_error = NULL,
                    updated_at = now()
                WHERE id = :id AND status <> :deleting
                """, parameters(
                "status", RagDocumentStatus.INDEXED.name(),
                "indexedGeneration", indexedGeneration,
                "chunkCount", chunkCount,
                "indexedAt", Timestamp.from(indexedAt.toInstant())
        ));
    }

    @Override
    public void markFailed(Long id, String errorMessage) {
        updateUnlessDeleting(id, """
                UPDATE rag_documents
                SET status = :status, last_error = :errorMessage, updated_at = now()
                WHERE id = :id AND status <> :deleting
                """, parameters("status", RagDocumentStatus.FAILED.name(), "errorMessage", errorMessage));
    }

    @Override
    public void markDeleting(Long id, String note) {
        int updated = jdbcClient.sql("""
                UPDATE rag_documents
                SET status = :status, last_error = :note, updated_at = now()
                WHERE id = :id
                """)
                .param("status", RagDocumentStatus.DELETING.name())
                .param("note", note)
                .param("id", id)
                .update();
        if (updated == 0) {
            throw new IllegalArgumentException("RAG 文档不存在: " + id);
        }
    }

    @Override
    @Transactional
    public void deleteById(Long id) {
        for (String table : List.of(
                "rag_chunks",
                "rag_index_outbox",
                "rag_index_job_transitions",
                "rag_index_jobs"
        )) {
            jdbcClient.sql("DELETE FROM " + table + " WHERE document_id = :documentId")
                    .param("documentId", id)
                    .update();
        }
        int deleted = jdbcClient.sql("DELETE FROM rag_documents WHERE id = :id")
                .param("id", id)
                .update();
        if (deleted == 0) {
            throw new IllegalArgumentException("RAG 文档不存在: " + id);
        }
    }

    private List<RagDocumentRecord> findByStatusBefore(
            RagDocumentStatus status,
            String cutoffExpression,
            OffsetDateTime cutoff,
            int limit
    ) {
        String sql = "SELECT " + SELECT_COLUMNS + """
                 FROM rag_documents
                 WHERE status = :status AND %s <= :cutoff
                 ORDER BY %s ASC
                 LIMIT :limit
                """.formatted(cutoffExpression, cutoffExpression);
        return jdbcClient.sql(sql)
                .param("status", status.name())
                .param("cutoff", Timestamp.from(cutoff.toInstant()))
                .param("limit", limit)
                .query(this::mapDocument)
                .list();
    }

    private Optional<RagDocumentRecord> findLatestMatching(
            String sourceType,
            String sourceUri,
            String externalRef,
            String title,
            String contentSha256
    ) {
        return jdbcClient.sql("SELECT " + SELECT_COLUMNS + """
                 FROM rag_documents
                 WHERE source_type = :sourceType
                   AND source_uri IS NOT DISTINCT FROM :sourceUri
                   AND external_ref IS NOT DISTINCT FROM :externalRef
                   AND title IS NOT DISTINCT FROM :title
                   AND content_sha256 = :contentSha256
                 ORDER BY id DESC
                 LIMIT 1
                """)
                .param("sourceType", sourceType)
                .param("sourceUri", sourceUri)
                .param("externalRef", externalRef)
                .param("title", title)
                .param("contentSha256", contentSha256)
                .query(this::mapDocument)
                .optional();
    }

    private void updateUnlessDeleting(Long id, String sql, Map<String, ?> parameters) {
        JdbcClient.StatementSpec statement = jdbcClient.sql(sql)
                .params(parameters)
                .param("id", id)
                .param("deleting", RagDocumentStatus.DELETING.name());
        if (statement.update() > 0) {
            return;
        }
        Optional<RagDocumentRecord> current = findById(id);
        if (current.isPresent() && RagDocumentStatus.DELETING.name().equals(current.get().status())) {
            return;
        }
        throw new IllegalArgumentException("RAG 文档不存在: " + id);
    }

    private Map<String, Object> parameters(Object... namesAndValues) {
        Map<String, Object> parameters = new LinkedHashMap<>();
        for (int index = 0; index < namesAndValues.length; index += 2) {
            parameters.put((String) namesAndValues[index], namesAndValues[index + 1]);
        }
        return parameters;
    }

    private RagDocumentRecord mapDocument(ResultSet resultSet, int rowNumber) throws SQLException {
        Integer chunkCount = RagJdbcRows.intValue(resultSet, "chunk_count");
        Integer attemptCount = RagJdbcRows.intValue(resultSet, "attempt_count");
        return new RagDocumentRecord(
                RagJdbcRows.longValue(resultSet, "id"),
                RagJdbcRows.string(resultSet, "source_type"),
                RagJdbcRows.string(resultSet, "source_uri"),
                RagJdbcRows.string(resultSet, "external_ref"),
                RagJdbcRows.string(resultSet, "title"),
                RagJdbcRows.string(resultSet, "content"),
                RagJdbcRows.string(resultSet, "content_sha256"),
                RagJdbcRows.longValue(resultSet, "indexed_generation"),
                RagJdbcRows.string(resultSet, "status"),
                chunkCount == null ? 0 : chunkCount,
                attemptCount == null ? 0 : attemptCount,
                RagJdbcRows.jsonMap(jsonCodec, resultSet, "metadata"),
                RagJdbcRows.string(resultSet, "last_error"),
                RagJdbcRows.offsetDateTime(resultSet, "last_attempted_at"),
                RagJdbcRows.offsetDateTime(resultSet, "indexed_at"),
                RagJdbcRows.offsetDateTime(resultSet, "created_at"),
                RagJdbcRows.offsetDateTime(resultSet, "updated_at")
        );
    }
}
