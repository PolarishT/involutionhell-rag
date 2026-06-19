package com.involutionhell.backend.rag.retrieval.persistence.jdbc;

import com.involutionhell.backend.rag.retrieval.persistence.RagConversationCursor;
import com.involutionhell.backend.rag.retrieval.persistence.RagConversationPage;
import com.involutionhell.backend.rag.retrieval.persistence.RagConversationRecord;
import com.involutionhell.backend.rag.retrieval.persistence.RagConversationRepository;
import com.involutionhell.backend.rag.shared.persistence.RagJdbcRows;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

@Repository
public class JdbcRagConversationRepository implements RagConversationRepository {

    private static final String SELECT_COLUMNS = """
            id, conversation_id, user_id, title, status, message_count,
            created_at, updated_at, last_message_at
            """;

    private final JdbcClient jdbcClient;

    public JdbcRagConversationRepository(JdbcClient jdbcClient) {
        this.jdbcClient = jdbcClient;
    }

    @Override
    public Optional<RagConversationRecord> findByConversationId(String conversationId) {
        return jdbcClient.sql("SELECT " + SELECT_COLUMNS + """
                 FROM rag_conversations
                 WHERE conversation_id = :conversationId
                 LIMIT 1
                """)
                .param("conversationId", conversationId)
                .query(this::mapConversation)
                .optional();
    }

    @Override
    public Optional<RagConversationRecord> findByUserIdAndConversationId(String userId, String conversationId) {
        return jdbcClient.sql("SELECT " + SELECT_COLUMNS + """
                 FROM rag_conversations
                 WHERE user_id = :userId AND conversation_id = :conversationId
                 LIMIT 1
                """)
                .param("userId", userId)
                .param("conversationId", conversationId)
                .query(this::mapConversation)
                .optional();
    }

    @Override
    @Transactional(propagation = Propagation.NESTED)
    public RagConversationRecord create(String userId, String conversationId, String title) {
        jdbcClient.sql("""
                INSERT INTO rag_conversations (conversation_id, user_id, title)
                VALUES (:conversationId, :userId, :title)
                """)
                .param("conversationId", conversationId)
                .param("userId", userId)
                .param("title", title)
                .update();
        return findByUserIdAndConversationId(userId, conversationId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "会话不存在"));
    }

    @Override
    public RagConversationPage findByUserId(String userId, int limit, RagConversationCursor cursor) {
        String cursorCondition = "";
        Map<String, Object> parameters = new LinkedHashMap<>();
        parameters.put("userId", userId);
        parameters.put("limit", limit + 1);
        if (cursor != null && cursor.id() != null) {
            parameters.put("cursorId", cursor.id());
            if (cursor.lastMessageAt() == null) {
                cursorCondition = " AND last_message_at IS NULL AND id < :cursorId";
            } else {
                cursorCondition = """
                         AND (
                              last_message_at < :cursorTime
                           OR (last_message_at = :cursorTime AND id < :cursorId)
                           OR last_message_at IS NULL
                         )
                        """;
                parameters.put("cursorTime", Timestamp.from(cursor.lastMessageAt().toInstant()));
            }
        }
        List<RagConversationRecord> rows = new ArrayList<>(jdbcClient.sql("SELECT " + SELECT_COLUMNS + """
                 FROM rag_conversations
                 WHERE user_id = :userId AND status <> 'DELETED'
                """ + cursorCondition + """
                 ORDER BY last_message_at DESC NULLS LAST, id DESC
                 LIMIT :limit
                """)
                .params(parameters)
                .query(this::mapConversation)
                .list());
        RagConversationCursor nextCursor = null;
        if (rows.size() > limit) {
            RagConversationRecord last = rows.get(limit - 1);
            nextCursor = new RagConversationCursor(last.lastMessageAt(), last.id());
            rows = new ArrayList<>(rows.subList(0, limit));
        }
        return new RagConversationPage(rows, nextCursor);
    }

    @Override
    @Transactional
    public RagConversationRecord update(String userId, String conversationId, String title, String status) {
        if (title == null && status == null) {
            return requireConversation(userId, conversationId);
        }
        List<String> assignments = new ArrayList<>();
        Map<String, Object> parameters = new LinkedHashMap<>();
        if (title != null) {
            assignments.add("title = :title");
            parameters.put("title", title);
        }
        if (status != null) {
            assignments.add("status = :status");
            parameters.put("status", status);
        }
        assignments.add("updated_at = now()");
        parameters.put("userId", userId);
        parameters.put("conversationId", conversationId);
        int updated = jdbcClient.sql("""
                UPDATE rag_conversations
                SET %s
                WHERE user_id = :userId AND conversation_id = :conversationId
                """.formatted(String.join(", ", assignments)))
                .params(parameters)
                .update();
        if (updated == 0) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "会话不存在");
        }
        return requireConversation(userId, conversationId);
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public void lockById(Long id) {
        jdbcClient.sql("SELECT id FROM rag_conversations WHERE id = :id FOR UPDATE")
                .param("id", id)
                .query(Long.class)
                .single();
    }

    @Override
    public void refreshStats(Long id) {
        jdbcClient.sql("""
                UPDATE rag_conversations
                SET message_count = (
                        SELECT COUNT(*) FROM rag_conversation_messages WHERE conversation_id = :id
                    ),
                    last_message_at = (
                        SELECT MAX(created_at) FROM rag_conversation_messages WHERE conversation_id = :id
                    ),
                    updated_at = now()
                WHERE id = :id
                """)
                .param("id", id)
                .update();
    }

    private RagConversationRecord requireConversation(String userId, String conversationId) {
        return findByUserIdAndConversationId(userId, conversationId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "会话不存在"));
    }

    private RagConversationRecord mapConversation(ResultSet resultSet, int rowNumber) throws SQLException {
        Integer messageCount = RagJdbcRows.intValue(resultSet, "message_count");
        return new RagConversationRecord(
                RagJdbcRows.longValue(resultSet, "id"),
                RagJdbcRows.string(resultSet, "conversation_id"),
                RagJdbcRows.string(resultSet, "user_id"),
                RagJdbcRows.string(resultSet, "title"),
                RagJdbcRows.string(resultSet, "status"),
                messageCount == null ? 0 : messageCount,
                RagJdbcRows.offsetDateTime(resultSet, "created_at"),
                RagJdbcRows.offsetDateTime(resultSet, "updated_at"),
                RagJdbcRows.offsetDateTime(resultSet, "last_message_at")
        );
    }
}
