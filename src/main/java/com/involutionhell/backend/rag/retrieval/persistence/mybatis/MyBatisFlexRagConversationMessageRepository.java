package com.involutionhell.backend.rag.retrieval.persistence.mybatis;

import com.involutionhell.backend.rag.retrieval.persistence.RagConversationMessageRecord;
import com.involutionhell.backend.rag.retrieval.persistence.RagConversationMessageRepository;
import com.mybatisflex.core.query.QueryChain;
import com.mybatisflex.core.query.RawQueryColumn;
import com.mybatisflex.core.row.DbChain;
import com.mybatisflex.core.update.UpdateChain;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

@Repository
public class MyBatisFlexRagConversationMessageRepository implements RagConversationMessageRepository {

    private final RagConversationMessageMapper messageMapper;

    public MyBatisFlexRagConversationMessageRepository(RagConversationMessageMapper messageMapper) {
        this.messageMapper = messageMapper;
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
        Integer nextSequence = DbChain.table("rag_conversation_messages")
                .select(new RawQueryColumn("COALESCE(MAX(sequence_no), 0) + 1"))
                .where("conversation_id = ?", conversationId)
                .objAsOpt(Integer.class)
                .orElse(1);
        String messageId = "msg:" + UUID.randomUUID();
        DbChain.table("rag_conversation_messages")
                .set("message_id", messageId)
                .set("conversation_id", conversationId)
                .set("role", role)
                .set("content", content)
                .set("status", status)
                .set("correlation_id", correlationId)
                .set("sequence_no", nextSequence)
                .save();
        return findByMessageId(messageId);
    }

    @Override
    public List<RagConversationMessageRecord> findByConversationId(Long conversationId) {
        return QueryChain.of(messageMapper)
                .where("conversation_id = ?", conversationId)
                .orderBy("sequence_no ASC")
                .list()
                .stream()
                .map(this::toRecord)
                .toList();
    }

    @Override
    public List<RagConversationMessageRecord> findRecentByConversationId(Long conversationId, int limit) {
        return QueryChain.of(messageMapper)
                .where("conversation_id = ?", conversationId)
                .orderBy("sequence_no DESC")
                .limit(limit)
                .list()
                .stream()
                .map(this::toRecord)
                .sorted(Comparator.comparingInt(RagConversationMessageRecord::sequenceNo))
                .toList();
    }

    @Override
    public RagConversationMessageRecord findById(Long id) {
        return QueryChain.of(messageMapper)
                .where("id = ?", id)
                .limit(1)
                .oneOpt()
                .map(this::toRecord)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "消息不存在"));
    }

    @Override
    @Transactional
    public RagConversationMessageRecord updateContentAndStatus(Long id, String content, String status) {
        UpdateChain.of(messageMapper)
                .set("content", content, true)
                .set("status", status, true)
                .where("id = ?", id)
                .update();
        return findById(id);
    }

    private RagConversationMessageRecord findByMessageId(String messageId) {
        return QueryChain.of(messageMapper)
                .where("message_id = ?", messageId)
                .limit(1)
                .oneOpt()
                .map(this::toRecord)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "消息不存在"));
    }

    private RagConversationMessageRecord toRecord(RagConversationMessageEntity entity) {
        return new RagConversationMessageRecord(
                entity.getId(),
                entity.getMessageId(),
                entity.getConversationId(),
                entity.getRole(),
                entity.getContent(),
                entity.getStatus(),
                entity.getTokenCount(),
                entity.getCorrelationId(),
                entity.getSequenceNo() == null ? 0 : entity.getSequenceNo(),
                entity.getCreatedAt()
        );
    }
}
