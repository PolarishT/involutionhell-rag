package com.involutionhell.backend.rag.infrastructure.nativeimage;

import com.involutionhell.backend.rag.common.api.ApiResponse;
import com.involutionhell.backend.rag.common.api.DataResponse;
import com.involutionhell.backend.rag.document.api.RagDocumentCreateRequest;
import com.involutionhell.backend.rag.document.api.RagDocumentUpdateRequest;
import com.involutionhell.backend.rag.document.api.RagDocumentView;
import com.involutionhell.backend.rag.indexing.api.RagIndexJobView;
import com.involutionhell.backend.rag.indexing.api.RagIndexOutboxView;
import com.involutionhell.backend.rag.indexing.api.RagIndexTimelineDocumentView;
import com.involutionhell.backend.rag.indexing.api.RagIndexTimelineView;
import com.involutionhell.backend.rag.indexing.api.RagIndexTransitionView;
import com.involutionhell.backend.rag.indexing.messaging.RagIndexMessage;
import com.involutionhell.backend.rag.retrieval.api.RagAnswerResponse;
import com.involutionhell.backend.rag.retrieval.api.RagAskRequest;
import com.involutionhell.backend.rag.retrieval.api.RagContextView;
import com.involutionhell.backend.rag.retrieval.api.RagConversationListView;
import com.involutionhell.backend.rag.retrieval.api.RagConversationMessage;
import com.involutionhell.backend.rag.retrieval.api.RagConversationMessageView;
import com.involutionhell.backend.rag.retrieval.api.RagConversationMessagesView;
import com.involutionhell.backend.rag.retrieval.api.RagConversationSummaryView;
import com.involutionhell.backend.rag.retrieval.api.RagConversationUpdateRequest;
import com.involutionhell.backend.rag.retrieval.api.RagResponseNoticeView;
import com.involutionhell.backend.rag.shared.properties.RagProperties;
import org.springframework.aot.hint.BindingReflectionHintsRegistrar;
import org.springframework.aot.hint.MemberCategory;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.RuntimeHintsRegistrar;
import org.springframework.aot.hint.TypeReference;

/**
 * 为 native image 显式注册当前应用里需要运行时绑定的类型。
 *
 * <p>这里主要覆盖三类对象：
 * 1. Controller 层请求/响应 DTO；
 * 2. RocketMQ 通过统一 JSON codec 读写的消息体；
 * 3. {@link RagProperties} 及其嵌套配置记录。
 */
public class RagRuntimeHints implements RuntimeHintsRegistrar {

    private static final BindingReflectionHintsRegistrar BINDING_HINTS =
            new BindingReflectionHintsRegistrar();

    @Override
    public void registerHints(RuntimeHints hints, ClassLoader classLoader) {
        BINDING_HINTS.registerReflectionHints(
                hints.reflection(),
                ApiResponse.class,
                DataResponse.class,
                RagAskRequest.class,
                RagDocumentCreateRequest.class,
                RagDocumentUpdateRequest.class,
                RagAnswerResponse.class,
                RagDocumentView.class,
                RagIndexJobView.class,
                RagIndexTimelineDocumentView.class,
                RagIndexTimelineView.class,
                RagContextView.class,
                RagConversationListView.class,
                RagConversationMessage.class,
                RagConversationMessageView.class,
                RagConversationMessagesView.class,
                RagConversationSummaryView.class,
                RagConversationUpdateRequest.class,
                RagResponseNoticeView.class,
                RagIndexOutboxView.class,
                RagIndexTransitionView.class,
                RagIndexMessage.class,
                RagProperties.class
        );
        registerMyBatisFlexTypes(hints, classLoader);
    }

    private void registerMyBatisFlexTypes(RuntimeHints hints, ClassLoader classLoader) {
        String[] entities = {
                "com.involutionhell.backend.rag.document.persistence.mybatis.RagDocumentEntity",
                "com.involutionhell.backend.rag.indexing.persistence.mybatis.RagChunkEntity",
                "com.involutionhell.backend.rag.indexing.persistence.mybatis.RagEmbeddingCacheEntity",
                "com.involutionhell.backend.rag.indexing.persistence.mybatis.RagIndexJobEntity",
                "com.involutionhell.backend.rag.indexing.persistence.mybatis.RagIndexJobTransitionEntity",
                "com.involutionhell.backend.rag.indexing.persistence.mybatis.RagIndexMessageFailureEntity",
                "com.involutionhell.backend.rag.indexing.persistence.mybatis.RagIndexOutboxEntity",
                "com.involutionhell.backend.rag.retrieval.persistence.mybatis.RagAskRunEntity",
                "com.involutionhell.backend.rag.retrieval.persistence.mybatis.RagConversationEntity",
                "com.involutionhell.backend.rag.retrieval.persistence.mybatis.RagConversationMessageEntity",
                "com.involutionhell.backend.rag.retrieval.persistence.mybatis.RagUserEntity"
        };
        for (String entity : entities) {
            hints.reflection().registerTypeIfPresent(
                    classLoader,
                    entity,
                    MemberCategory.INVOKE_PUBLIC_CONSTRUCTORS,
                    MemberCategory.INTROSPECT_PUBLIC_METHODS,
                    MemberCategory.INVOKE_PUBLIC_METHODS,
                    MemberCategory.DECLARED_FIELDS
            );
        }

        String[] records = {
                "com.involutionhell.backend.rag.document.persistence.RagDocumentRecord",
                "com.involutionhell.backend.rag.indexing.persistence.RagChunkRecord",
                "com.involutionhell.backend.rag.indexing.persistence.RagChunkSearchRecord",
                "com.involutionhell.backend.rag.indexing.persistence.RagIndexJobRecord",
                "com.involutionhell.backend.rag.indexing.persistence.RagIndexJobTransitionRecord",
                "com.involutionhell.backend.rag.indexing.persistence.RagIndexMessageFailureRecord",
                "com.involutionhell.backend.rag.indexing.persistence.RagIndexOutboxRecord",
                "com.involutionhell.backend.rag.retrieval.persistence.RagAskRunRecord",
                "com.involutionhell.backend.rag.retrieval.persistence.RagConversationCursor",
                "com.involutionhell.backend.rag.retrieval.persistence.RagConversationMessageCursor",
                "com.involutionhell.backend.rag.retrieval.persistence.RagConversationMessagePage",
                "com.involutionhell.backend.rag.retrieval.persistence.RagConversationMessageRecord",
                "com.involutionhell.backend.rag.retrieval.persistence.RagConversationPage",
                "com.involutionhell.backend.rag.retrieval.persistence.RagConversationRecord",
                "com.involutionhell.backend.rag.retrieval.persistence.RagStaleAskRunRecord"
        };
        for (String record : records) {
            hints.reflection().registerTypeIfPresent(
                    classLoader,
                    record,
                    MemberCategory.INTROSPECT_PUBLIC_CONSTRUCTORS,
                    MemberCategory.INVOKE_PUBLIC_CONSTRUCTORS,
                    MemberCategory.INTROSPECT_PUBLIC_METHODS,
                    MemberCategory.INVOKE_PUBLIC_METHODS
            );
        }

        String[] mappers = {
                "com.involutionhell.backend.rag.document.persistence.mybatis.RagDocumentMapper",
                "com.involutionhell.backend.rag.indexing.persistence.mybatis.RagChunkMapper",
                "com.involutionhell.backend.rag.indexing.persistence.mybatis.RagEmbeddingCacheMapper",
                "com.involutionhell.backend.rag.indexing.persistence.mybatis.RagIndexJobMapper",
                "com.involutionhell.backend.rag.indexing.persistence.mybatis.RagIndexJobTransitionMapper",
                "com.involutionhell.backend.rag.indexing.persistence.mybatis.RagIndexMessageFailureMapper",
                "com.involutionhell.backend.rag.indexing.persistence.mybatis.RagIndexOutboxMapper",
                "com.involutionhell.backend.rag.retrieval.persistence.mybatis.RagAskRunMapper",
                "com.involutionhell.backend.rag.retrieval.persistence.mybatis.RagConversationMapper",
                "com.involutionhell.backend.rag.retrieval.persistence.mybatis.RagConversationMessageMapper",
                "com.involutionhell.backend.rag.retrieval.persistence.mybatis.RagUserMapper"
        };
        for (String mapper : mappers) {
            hints.reflection().registerTypeIfPresent(
                    classLoader,
                    mapper,
                    MemberCategory.INTROSPECT_PUBLIC_METHODS,
                    MemberCategory.INVOKE_PUBLIC_METHODS
            );
            hints.proxies().registerJdkProxy(TypeReference.of(mapper));
        }
    }
}
