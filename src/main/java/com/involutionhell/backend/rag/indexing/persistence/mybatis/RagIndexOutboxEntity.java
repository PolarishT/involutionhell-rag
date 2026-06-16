package com.involutionhell.backend.rag.indexing.persistence.mybatis;

import com.mybatisflex.annotation.Id;
import com.mybatisflex.annotation.KeyType;
import com.mybatisflex.annotation.Table;
import java.time.OffsetDateTime;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Table("rag_index_outbox")
public class RagIndexOutboxEntity {

    @Id(value = "id", keyType = KeyType.Auto)
    private Long id;

    private Long documentId;
    private String contentSha256;
    private String eventType;
    private String status;
    private Integer attemptCount;
    private String messageId;
    private String lastError;
    private OffsetDateTime nextAttemptAt;
    private OffsetDateTime dispatchedAt;
    private OffsetDateTime consumedAt;
    private OffsetDateTime createdAt;
    private OffsetDateTime updatedAt;
}
