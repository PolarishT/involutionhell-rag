package com.involutionhell.backend.rag.retrieval.persistence.mybatis;

import com.involutionhell.backend.rag.shared.persistence.JsonStringTypeHandler;
import com.mybatisflex.annotation.Column;
import com.mybatisflex.annotation.Id;
import com.mybatisflex.annotation.KeyType;
import com.mybatisflex.annotation.Table;
import java.time.OffsetDateTime;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Table("rag_conversations")
public class RagConversationEntity {

    @Id(value = "id", keyType = KeyType.Auto)
    private Long id;

    private String conversationId;
    private String userId;
    private String title;
    private String status;
    private Integer messageCount;

    @Column(value = "metadata", typeHandler = JsonStringTypeHandler.class)
    private String metadata;

    private OffsetDateTime createdAt;
    private OffsetDateTime updatedAt;
    private OffsetDateTime lastMessageAt;
}
