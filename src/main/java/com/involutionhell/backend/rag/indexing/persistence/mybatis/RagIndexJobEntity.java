package com.involutionhell.backend.rag.indexing.persistence.mybatis;

import com.mybatisflex.annotation.Id;
import com.mybatisflex.annotation.KeyType;
import com.mybatisflex.annotation.Table;
import java.time.OffsetDateTime;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Table("rag_index_jobs")
public class RagIndexJobEntity {

    @Id(value = "id", keyType = KeyType.Auto)
    private Long id;

    private Long documentId;
    private String contentSha256;
    private String status;
    private String stage;
    private Long version;
    private String lastEvent;
    private Integer attemptCount;
    private Long targetGeneration;
    private String messageId;
    private String lastError;
    private OffsetDateTime startedAt;
    private OffsetDateTime finishedAt;
    private OffsetDateTime createdAt;
    private OffsetDateTime updatedAt;
}
