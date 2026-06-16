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
@Table("rag_users")
public class RagUserEntity {

    @Id(value = "id", keyType = KeyType.Auto)
    private Long id;

    private String userId;

    @Column(value = "metadata", typeHandler = JsonStringTypeHandler.class)
    private String metadata;

    private OffsetDateTime firstSeenAt;
    private OffsetDateTime lastSeenAt;
}
