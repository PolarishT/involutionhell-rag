package com.involutionhell.backend.rag.indexing.persistence.mybatis;

import com.mybatisflex.annotation.Id;
import com.mybatisflex.annotation.KeyType;
import com.mybatisflex.annotation.Table;
import java.time.OffsetDateTime;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Table("rag_embedding_cache")
public class RagEmbeddingCacheEntity {

    @Id(value = "chunk_hash", keyType = KeyType.None)
    private String chunkHash;

    @Id(value = "embedding_model", keyType = KeyType.None)
    private String embeddingModel;

    @Id(value = "embedding_dimension", keyType = KeyType.None)
    private Integer embeddingDimension;

    private String embeddingJson;
    private OffsetDateTime createdAt;
    private OffsetDateTime updatedAt;
}
