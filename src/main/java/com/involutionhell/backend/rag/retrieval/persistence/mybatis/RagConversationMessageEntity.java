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
@Table("rag_conversation_messages")
public class RagConversationMessageEntity {

    @Id(value = "id", keyType = KeyType.Auto)
    private Long id;

    private String messageId;
    private Long conversationId;
    private String role;
    private String content;
    private String status;
    private Integer tokenCount;
    private String correlationId;
    private Integer sequenceNo;

    @Column(value = "metadata", typeHandler = JsonStringTypeHandler.class)
    private String metadata;

    private OffsetDateTime createdAt;
}
