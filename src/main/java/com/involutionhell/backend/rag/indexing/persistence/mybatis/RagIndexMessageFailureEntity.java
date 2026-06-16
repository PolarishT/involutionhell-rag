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
@Table("rag_index_message_failures")
public class RagIndexMessageFailureEntity {

    @Id(value = "id", keyType = KeyType.Auto)
    private Long id;

    private String messageId;
    private String topic;
    private Integer deliveryAttempt;
    private String failureType;
    private String errorMessage;
    private String payloadBase64;
    private String payloadPreview;

    @Column(value = "properties_json", typeHandler = JsonStringTypeHandler.class)
    private String propertiesJson;

    private OffsetDateTime createdAt;
}
