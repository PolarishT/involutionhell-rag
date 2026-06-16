package com.involutionhell.backend.rag.retrieval.persistence.mybatis;

import com.involutionhell.backend.rag.retrieval.persistence.RagConversationCursor;
import com.involutionhell.backend.rag.retrieval.persistence.RagConversationPage;
import com.involutionhell.backend.rag.retrieval.persistence.RagConversationRecord;
import com.involutionhell.backend.rag.retrieval.persistence.RagConversationRepository;
import com.mybatisflex.core.query.QueryChain;
import com.mybatisflex.core.row.DbChain;
import com.mybatisflex.core.update.UpdateChain;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

@Repository
public class MyBatisFlexRagConversationRepository implements RagConversationRepository {

    private final RagConversationMapper conversationMapper;

    public MyBatisFlexRagConversationRepository(RagConversationMapper conversationMapper) {
        this.conversationMapper = conversationMapper;
    }

    @Override
    public Optional<RagConversationRecord> findByConversationId(String conversationId) {
        return QueryChain.of(conversationMapper)
                .where("conversation_id = ?", conversationId)
                .limit(1)
                .oneOpt()
                .map(this::toRecord);
    }

    @Override
    public Optional<RagConversationRecord> findByUserIdAndConversationId(String userId, String conversationId) {
        return QueryChain.of(conversationMapper)
                .where("user_id = ? AND conversation_id = ?", userId, conversationId)
                .limit(1)
                .oneOpt()
                .map(this::toRecord);
    }

    @Override
    @Transactional(propagation = Propagation.NESTED)
    public RagConversationRecord create(String userId, String conversationId, String title) {
        DbChain.table("rag_conversations")
                .set("conversation_id", conversationId)
                .set("user_id", userId)
                .set("title", title)
                .save();
        return findByUserIdAndConversationId(userId, conversationId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "会话不存在"));
    }

    @Override
    public RagConversationPage findByUserId(String userId, int limit, RagConversationCursor cursor) {
        QueryChain<RagConversationEntity> query = QueryChain.of(conversationMapper)
                .where("user_id = ? AND status <> ?", userId, "DELETED");
        if (cursor != null && cursor.id() != null) {
            if (cursor.lastMessageAt() == null) {
                query.and("last_message_at IS NULL AND id < ?", cursor.id());
            } else {
                Timestamp timestamp = Timestamp.from(cursor.lastMessageAt().toInstant());
                query.and("""
                        (
                             last_message_at < ?
                          OR (last_message_at = ? AND id < ?)
                          OR last_message_at IS NULL
                        )
                        """, timestamp, timestamp, cursor.id());
            }
        }
        List<RagConversationRecord> rows = query
                .orderBy("last_message_at DESC NULLS LAST", "id DESC")
                .limit(limit + 1)
                .list()
                .stream()
                .map(this::toRecord)
                .collect(java.util.stream.Collectors.toCollection(ArrayList::new));
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
            return findByUserIdAndConversationId(userId, conversationId)
                    .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "会话不存在"));
        }
        UpdateChain<RagConversationEntity> update = UpdateChain.of(conversationMapper);
        if (title != null) {
            update.set("title", title, true);
        }
        if (status != null) {
            update.set("status", status, true);
        }
        boolean updated = update
                .setRaw("updated_at", "now()", true)
                .where("user_id = ? AND conversation_id = ?", userId, conversationId)
                .update();
        if (!updated) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "会话不存在");
        }
        return findByUserIdAndConversationId(userId, conversationId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "会话不存在"));
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public void lockById(Long id) {
        QueryChain.of(conversationMapper)
                .select("id")
                .where("id = ?", id)
                .forUpdate()
                .limit(1)
                .one();
    }

    @Override
    public void refreshStats(Long id) {
        UpdateChain.of(conversationMapper)
                .setRaw("message_count", "(SELECT COUNT(*) FROM rag_conversation_messages WHERE conversation_id = " + id + ")", true)
                .setRaw("last_message_at", "(SELECT MAX(created_at) FROM rag_conversation_messages WHERE conversation_id = " + id + ")", true)
                .setRaw("updated_at", "now()", true)
                .where("id = ?", id)
                .update();
    }

    private RagConversationRecord toRecord(RagConversationEntity entity) {
        return new RagConversationRecord(
                entity.getId(),
                entity.getConversationId(),
                entity.getUserId(),
                entity.getTitle(),
                entity.getStatus(),
                entity.getMessageCount() == null ? 0 : entity.getMessageCount(),
                entity.getCreatedAt(),
                entity.getUpdatedAt(),
                entity.getLastMessageAt()
        );
    }
}
