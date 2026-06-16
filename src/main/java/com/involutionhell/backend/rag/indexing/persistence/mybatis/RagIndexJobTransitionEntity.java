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
@Table("rag_index_job_transitions")
public class RagIndexJobTransitionEntity {

    @Id(value = "id", keyType = KeyType.Auto)
    private Long id;

    private Long documentId;
    private Long jobId;
    private Long outboxId;
    private String contentSha256;
    private String fromState;
    private String toState;
    private String event;
    private String triggerType;
    private String triggeredBy;
    private Boolean success;
    private String failureReason;
    private String errorMessage;
    private String messageId;

    @Column(value = "metadata", typeHandler = JsonStringTypeHandler.class)
    private String metadata;

    private OffsetDateTime createdAt;
}
