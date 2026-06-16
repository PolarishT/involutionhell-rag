package com.involutionhell.backend.rag.indexing.persistence.mybatis;

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
@Table("rag_chunks")
public class RagChunkEntity {

    @Id(value = "id", keyType = KeyType.Auto)
    private Long id;

    private Long documentId;
    private Long indexGeneration;
    private Integer chunkIndex;
    private String chunkText;
    private String chunkHash;
    private Integer charCount;
    private Integer tokenCount;
    private String vectorId;

    @Column(value = "metadata", typeHandler = JsonStringTypeHandler.class)
    private String metadata;

    private OffsetDateTime createdAt;
    private OffsetDateTime updatedAt;
}
