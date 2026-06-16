package com.involutionhell.backend.rag.document.persistence.mybatis;

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
@Table("rag_documents")
public class RagDocumentEntity {

    @Id(value = "id", keyType = KeyType.Auto)
    private Long id;

    private String sourceType;
    private String sourceUri;
    private String externalRef;
    private String title;
    private String content;
    private String contentSha256;
    private Long indexedGeneration;
    private String status;
    private Integer chunkCount;
    private Integer attemptCount;

    @Column(value = "metadata", typeHandler = JsonStringTypeHandler.class)
    private String metadata;

    private String lastError;
    private OffsetDateTime lastAttemptedAt;
    private OffsetDateTime indexedAt;
    private OffsetDateTime createdAt;
    private OffsetDateTime updatedAt;
}
