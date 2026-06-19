package com.involutionhell.backend.rag.retrieval.persistence.jdbc;

import com.involutionhell.backend.rag.retrieval.persistence.RagAskRunRecord;
import com.involutionhell.backend.rag.retrieval.persistence.RagAskRunRepository;
import com.involutionhell.backend.rag.retrieval.persistence.RagStaleAskRunRecord;
import com.involutionhell.backend.rag.shared.persistence.RagDatabaseDialect;
import com.involutionhell.backend.rag.shared.persistence.RagJdbcJson;
import com.involutionhell.backend.rag.shared.persistence.RagJdbcRows;
import com.involutionhell.backend.rag.shared.support.RagJsonCodec;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
public class JdbcRagAskRunRepository implements RagAskRunRepository {

    private static final String SELECT_COLUMNS = """
            id, run_id, correlation_id, user_id, conversation_id, user_message_id,
            assistant_message_id, request_id, question, status, error_code, error_message
            """;

    private final JdbcClient jdbcClient;
    private final RagJsonCodec jsonCodec;
    private final RagDatabaseDialect databaseDialect;

    public JdbcRagAskRunRepository(
            JdbcClient jdbcClient,
            RagJsonCodec jsonCodec,
            RagDatabaseDialect databaseDialect
    ) {
        this.jdbcClient = jdbcClient;
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
        String filtersParameter = RagJdbcJson.parameter(databaseDialect, "filters");
        jdbcClient.sql("""
                INSERT INTO rag_ask_runs (
                    run_id, correlation_id, user_id, conversation_id, user_message_id,
                    assistant_message_id, request_id, question, top_k, status, filters
                ) VALUES (
                    :runId, :correlationId, :userId, :conversationId, :userMessageId,
                    :assistantMessageId, :requestId, :question, :topK, 'RUNNING', %s
                )
                """.formatted(filtersParameter))
                .param("runId", runId)
                .param("correlationId", correlationId)
                .param("userId", userId)
                .param("conversationId", conversationId)
                .param("userMessageId", userMessageId)
                .param("assistantMessageId", assistantMessageId)
                .param("requestId", requestId)
                .param("question", question)
                .param("topK", topK)
                .param("filters", jsonCodec.write(filters == null ? Map.of() : filters))
                .update();
    }

    @Override
    public Optional<RagAskRunRecord> findByRequestId(String userId, Long conversationId, String requestId) {
        return jdbcClient.sql("SELECT " + SELECT_COLUMNS + """
                 FROM rag_ask_runs
                 WHERE user_id = :userId
                   AND conversation_id = :conversationId
                   AND request_id = :requestId
                 LIMIT 1
                """)
                .param("userId", userId)
                .param("conversationId", conversationId)
                .param("requestId", requestId)
                .query(this::mapAskRun)
                .optional();
    }

    @Override
    public List<RagStaleAskRunRecord> findStaleRunning(OffsetDateTime startedBefore, int limit) {
        return jdbcClient.sql("""
                SELECT run_id, conversation_id, assistant_message_id, started_at
                FROM rag_ask_runs
                WHERE status = 'RUNNING' AND started_at < :startedBefore
                ORDER BY started_at ASC
                LIMIT :limit
                """)
                .param("startedBefore", Timestamp.from(startedBefore.toInstant()))
                .param("limit", limit)
                .query(this::mapStaleAskRun)
                .list();
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
        String retrievalQueriesParameter = RagJdbcJson.parameter(databaseDialect, "retrievalQueries");
        String retrievedContextsParameter = RagJdbcJson.parameter(databaseDialect, "retrievedContexts");
        String noticesParameter = RagJdbcJson.parameter(databaseDialect, "notices");
        jdbcClient.sql("""
                UPDATE rag_ask_runs
                SET assistant_message_id = :assistantMessageId,
                    retrieval_question = :retrievalQuestion,
                    retrieval_queries = %s,
                    retrieved_contexts = %s,
                    notices = %s,
                    generated_by_model = :generatedByModel,
                    degraded = :degraded,
                    status = 'SUCCEEDED',
                    error_code = NULL,
                    error_message = NULL,
                    completed_at = now()
                WHERE run_id = :runId AND status = 'RUNNING'
                """.formatted(retrievalQueriesParameter, retrievedContextsParameter, noticesParameter))
                .param("assistantMessageId", assistantMessageId)
                .param("retrievalQuestion", retrievalQuestion)
                .param("retrievalQueries", jsonCodec.write(retrievalQueries == null ? List.of() : retrievalQueries))
                .param("retrievedContexts", jsonCodec.write(retrievedContexts == null ? List.of() : retrievedContexts))
                .param("notices", jsonCodec.write(notices == null ? List.of() : notices))
                .param("generatedByModel", generatedByModel)
                .param("degraded", degraded)
                .param("runId", runId)
                .update();
    }

    @Override
    public void markFailed(String runId, String errorCode, String errorMessage, Object notices) {
        String noticesParameter = RagJdbcJson.parameter(databaseDialect, "notices");
        jdbcClient.sql("""
                UPDATE rag_ask_runs
                SET degraded = TRUE,
                    status = 'FAILED',
                    error_code = :errorCode,
                    error_message = :errorMessage,
                    notices = %s,
                    completed_at = now()
                WHERE run_id = :runId AND status = 'RUNNING'
                """.formatted(noticesParameter))
                .param("errorCode", errorCode)
                .param("errorMessage", errorMessage)
                .param("notices", jsonCodec.write(notices == null ? List.of() : notices))
                .param("runId", runId)
                .update();
    }

    private RagAskRunRecord mapAskRun(ResultSet resultSet, int rowNumber) throws SQLException {
        return new RagAskRunRecord(
                RagJdbcRows.longValue(resultSet, "id"),
                RagJdbcRows.string(resultSet, "run_id"),
                RagJdbcRows.string(resultSet, "correlation_id"),
                RagJdbcRows.string(resultSet, "user_id"),
                RagJdbcRows.longValue(resultSet, "conversation_id"),
                RagJdbcRows.longValue(resultSet, "user_message_id"),
                RagJdbcRows.longValue(resultSet, "assistant_message_id"),
                RagJdbcRows.string(resultSet, "request_id"),
                RagJdbcRows.string(resultSet, "question"),
                RagJdbcRows.string(resultSet, "status"),
                RagJdbcRows.string(resultSet, "error_code"),
                RagJdbcRows.string(resultSet, "error_message")
        );
    }

    private RagStaleAskRunRecord mapStaleAskRun(ResultSet resultSet, int rowNumber) throws SQLException {
        return new RagStaleAskRunRecord(
                RagJdbcRows.string(resultSet, "run_id"),
                RagJdbcRows.longValue(resultSet, "conversation_id"),
                RagJdbcRows.longValue(resultSet, "assistant_message_id"),
                RagJdbcRows.offsetDateTime(resultSet, "started_at")
        );
    }
}
