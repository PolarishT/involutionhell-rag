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
@Table("rag_ask_runs")
public class RagAskRunEntity {

    @Id(value = "id", keyType = KeyType.Auto)
    private Long id;

    private String runId;
    private String correlationId;
    private String userId;
    private Long conversationId;
    private Long userMessageId;
    private Long assistantMessageId;
    private String requestId;
    private String question;
    private String retrievalQuestion;
    private Integer topK;

    @Column(value = "filters", typeHandler = JsonStringTypeHandler.class)
    private String filters;

    @Column(value = "retrieval_queries", typeHandler = JsonStringTypeHandler.class)
    private String retrievalQueries;

    @Column(value = "retrieved_contexts", typeHandler = JsonStringTypeHandler.class)
    private String retrievedContexts;

    @Column(value = "notices", typeHandler = JsonStringTypeHandler.class)
    private String notices;

    private Boolean generatedByModel;
    private Boolean degraded;
    private String status;
    private String errorCode;
    private String errorMessage;
    private OffsetDateTime startedAt;
    private OffsetDateTime completedAt;
}
