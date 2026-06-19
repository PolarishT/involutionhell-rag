package com.involutionhell.backend.rag.retrieval.persistence.jdbc;

import com.involutionhell.backend.rag.retrieval.persistence.RagConversationMessageCursor;
import com.involutionhell.backend.rag.retrieval.persistence.RagConversationMessagePage;
import com.involutionhell.backend.rag.retrieval.persistence.RagConversationMessageRecord;
import com.involutionhell.backend.rag.retrieval.persistence.RagConversationMessageRepository;
import com.involutionhell.backend.rag.shared.persistence.RagJdbcRows;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

@Repository
public class JdbcRagConversationMessageRepository implements RagConversationMessageRepository {

    private static final String SELECT_COLUMNS = """
            id, message_id, conversation_id, role, content, status, token_count,
            correlation_id, sequence_no, created_at
            """;

    private final JdbcClient jdbcClient;

    public JdbcRagConversationMessageRepository(JdbcClient jdbcClient) {
        this.jdbcClient = jdbcClient;
    }

    @Override
    @Transactional
    public RagConversationMessageRecord append(
            Long conversationId,
            String role,
            String content,
            String status,
            String correlationId
    ) {
        Integer nextSequence = jdbcClient.sql("""
                SELECT COALESCE(MAX(sequence_no), 0) + 1
                FROM rag_conversation_messages
                WHERE conversation_id = :conversationId
                """)
                .param("conversationId", conversationId)
                .query(Integer.class)
                .single();
        String messageId = "msg:" + UUID.randomUUID();
        jdbcClient.sql("""
                INSERT INTO rag_conversation_messages (
                    message_id, conversation_id, role, content, status, correlation_id, sequence_no
                ) VALUES (
                    :messageId, :conversationId, :role, :content, :status, :correlationId, :sequenceNo
                )
                """)
                .param("messageId", messageId)
                .param("conversationId", conversationId)
                .param("role", role)
                .param("content", content)
                .param("status", status)
                .param("correlationId", correlationId)
                .param("sequenceNo", nextSequence)
                .update();
        return findByMessageId(messageId);
    }

    @Override
    public List<RagConversationMessageRecord> findByConversationId(Long conversationId) {
        return jdbcClient.sql("SELECT " + SELECT_COLUMNS + """
                 FROM rag_conversation_messages
                 WHERE conversation_id = :conversationId AND role IN ('user', 'assistant')
                 ORDER BY sequence_no ASC
                """)
                .param("conversationId", conversationId)
                .query(this::mapMessage)
                .list();
    }

    @Override
    public RagConversationMessagePage findByConversationId(
            Long conversationId,
            int limit,
            RagConversationMessageCursor cursor
    ) {
        String cursorCondition = cursor != null && cursor.id() != null ? """
                 AND (
                      sequence_no > :cursorSequence
                   OR (sequence_no = :cursorSequence AND id > :cursorId)
                 )
                """ : "";
        JdbcClient.StatementSpec statement = jdbcClient.sql("SELECT " + SELECT_COLUMNS + """
                 FROM rag_conversation_messages
                 WHERE conversation_id = :conversationId
                   AND role IN ('user', 'assistant')
                """ + cursorCondition + """
                 ORDER BY sequence_no ASC, id ASC
                 LIMIT :limit
                """)
                .param("conversationId", conversationId)
                .param("limit", limit + 1);
        if (!cursorCondition.isEmpty()) {
            statement.param("cursorSequence", cursor.sequenceNo())
                    .param("cursorId", cursor.id());
        }
        List<RagConversationMessageRecord> rows = new ArrayList<>(statement.query(this::mapMessage).list());
        RagConversationMessageCursor nextCursor = null;
        if (rows.size() > limit) {
            RagConversationMessageRecord last = rows.get(limit - 1);
            nextCursor = new RagConversationMessageCursor(last.sequenceNo(), last.id());
            rows = new ArrayList<>(rows.subList(0, limit));
        }
        return new RagConversationMessagePage(rows, nextCursor);
    }

    @Override
    public List<RagConversationMessageRecord> findRecentByConversationId(Long conversationId, int limit) {
        return jdbcClient.sql("SELECT " + SELECT_COLUMNS + """
                 FROM rag_conversation_messages
                 WHERE conversation_id = :conversationId
                 ORDER BY sequence_no DESC
                 LIMIT :limit
                """)
                .param("conversationId", conversationId)
                .param("limit", limit)
                .query(this::mapMessage)
                .list()
                .stream()
                .sorted(Comparator.comparingInt(RagConversationMessageRecord::sequenceNo))
                .toList();
    }

    @Override
    public RagConversationMessageRecord findById(Long id) {
        return jdbcClient.sql("SELECT " + SELECT_COLUMNS + """
                 FROM rag_conversation_messages
                 WHERE id = :id
                 LIMIT 1
                """)
                .param("id", id)
                .query(this::mapMessage)
                .optional()
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "消息不存在"));
    }

    @Override
    @Transactional
    public RagConversationMessageRecord updateContentAndStatus(Long id, String content, String status) {
        jdbcClient.sql("""
                UPDATE rag_conversation_messages
                SET content = :content, status = :status
                WHERE id = :id
                """)
                .param("content", content)
                .param("status", status)
                .param("id", id)
                .update();
        return findById(id);
    }

    private RagConversationMessageRecord findByMessageId(String messageId) {
        return jdbcClient.sql("SELECT " + SELECT_COLUMNS + """
                 FROM rag_conversation_messages
                 WHERE message_id = :messageId
                 LIMIT 1
                """)
                .param("messageId", messageId)
                .query(this::mapMessage)
                .optional()
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "消息不存在"));
    }

    private RagConversationMessageRecord mapMessage(ResultSet resultSet, int rowNumber) throws SQLException {
        Integer sequenceNo = RagJdbcRows.intValue(resultSet, "sequence_no");
        return new RagConversationMessageRecord(
                RagJdbcRows.longValue(resultSet, "id"),
                RagJdbcRows.string(resultSet, "message_id"),
                RagJdbcRows.longValue(resultSet, "conversation_id"),
                RagJdbcRows.string(resultSet, "role"),
                RagJdbcRows.string(resultSet, "content"),
                RagJdbcRows.string(resultSet, "status"),
                RagJdbcRows.intValue(resultSet, "token_count"),
                RagJdbcRows.string(resultSet, "correlation_id"),
                sequenceNo == null ? 0 : sequenceNo,
                RagJdbcRows.offsetDateTime(resultSet, "created_at")
        );
    }
}
