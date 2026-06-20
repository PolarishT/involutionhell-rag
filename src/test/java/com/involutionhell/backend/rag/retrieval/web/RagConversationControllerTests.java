package com.involutionhell.backend.rag.retrieval.web;

import com.involutionhell.backend.rag.retrieval.api.RagConversationListView;
import com.involutionhell.backend.rag.retrieval.api.RagConversationMessageView;
import com.involutionhell.backend.rag.retrieval.api.RagConversationMessagesView;
import com.involutionhell.backend.rag.retrieval.api.RagConversationSummaryView;
import com.involutionhell.backend.rag.retrieval.api.RagConversationUpdateRequest;
import com.involutionhell.backend.rag.retrieval.application.RagConversationService;
import java.time.OffsetDateTime;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;

import static org.assertj.core.api.Assertions.assertThat;

class RagConversationControllerTests {

    private static final OffsetDateTime CREATED_AT = OffsetDateTime.parse("2026-06-17T00:00:00Z");
    private static final OffsetDateTime UPDATED_AT = OffsetDateTime.parse("2026-06-17T00:01:00Z");

    @Test
    void listConversationsUsesRequiredResponseShape() {
        StubConversationService service = new StubConversationService(new RagConversationListView(
                List.of(new RagConversationSummaryView(
                        "conversation-1",
                        "新对话",
                        CREATED_AT,
                        UPDATED_AT
                )),
                "cursor-2"
        ));
        WebTestClient client = WebTestClient
                .bindToController(new RagConversationController(service))
                .build();

        client.get()
                .uri(uriBuilder -> uriBuilder
                        .path("/public/rag/conversations")
                        .queryParam("userId", "codex-bruno-test")
                        .queryParam("limit", 50)
                        .queryParam("cursor", "cursor-1")
                        .build())
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.data.conversations[0].conversationId").isEqualTo("conversation-1")
                .jsonPath("$.data.conversations[0].title").isEqualTo("新对话")
                .jsonPath("$.data.conversations[0].createdAt").exists()
                .jsonPath("$.data.conversations[0].updatedAt").exists()
                .jsonPath("$.data.nextCursor").isEqualTo("cursor-2")
                .jsonPath("$.data.items").doesNotExist()
                .jsonPath("$.data.conversations[0].userId").doesNotExist()
                .jsonPath("$.data.conversations[0].status").doesNotExist()
                .jsonPath("$.data.conversations[0].messageCount").doesNotExist()
                .jsonPath("$.data.conversations[0].lastMessageAt").doesNotExist()
                .jsonPath("$.success").doesNotExist()
                .jsonPath("$.message").doesNotExist();

        assertThat(service.listUserId).isEqualTo("codex-bruno-test");
        assertThat(service.listLimit).isEqualTo(50);
        assertThat(service.listCursor).isEqualTo("cursor-1");
    }

    @Test
    void updateConversationReturnsRequiredSummaryShape() {
        StubConversationService service = new StubConversationService(new RagConversationSummaryView(
                "conversation-1",
                "新的标题",
                CREATED_AT,
                OffsetDateTime.parse("2026-06-17T00:02:00Z")
        ));
        WebTestClient client = WebTestClient
                .bindToController(new RagConversationController(service))
                .build();

        client.post()
                .uri("/public/rag/conversations/conversation-1/update")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("""
                        {
                          "userId": "codex-bruno-test",
                          "title": "新的标题"
                        }
                        """)
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.data.conversationId").isEqualTo("conversation-1")
                .jsonPath("$.data.title").isEqualTo("新的标题")
                .jsonPath("$.data.createdAt").exists()
                .jsonPath("$.data.updatedAt").exists()
                .jsonPath("$.data.userId").doesNotExist()
                .jsonPath("$.data.status").doesNotExist()
                .jsonPath("$.data.messageCount").doesNotExist()
                .jsonPath("$.data.lastMessageAt").doesNotExist()
                .jsonPath("$.success").doesNotExist()
                .jsonPath("$.message").doesNotExist();

        assertThat(service.updateConversationId).isEqualTo("conversation-1");
        assertThat(service.updateRequest.userId()).isEqualTo("codex-bruno-test");
        assertThat(service.updateRequest.title()).isEqualTo("新的标题");
    }

    @Test
    void getMessagesUsesCursorAndRequiredResponseShape() {
        StubConversationService service = new StubConversationService(new RagConversationMessagesView(
                "conversation-1",
                List.of(
                        new RagConversationMessageView(
                                "message-1",
                                "user",
                                "你好",
                                CREATED_AT
                        ),
                        new RagConversationMessageView(
                                "message-2",
                                "assistant",
                                "你好，有什么可以帮你?",
                                UPDATED_AT
                        )
                ),
                "cursor-2"
        ));
        WebTestClient client = WebTestClient
                .bindToController(new RagConversationController(service))
                .build();

        client.get()
                .uri(uriBuilder -> uriBuilder
                        .path("/public/rag/conversations/conversation-1/messages")
                        .queryParam("userId", "codex-bruno-test")
                        .queryParam("limit", 50)
                        .queryParam("cursor", "cursor-1")
                        .build())
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.data.conversationId").isEqualTo("conversation-1")
                .jsonPath("$.data.messages[0].id").isEqualTo("message-1")
                .jsonPath("$.data.messages[0].role").isEqualTo("user")
                .jsonPath("$.data.messages[0].content").isEqualTo("你好")
                .jsonPath("$.data.messages[0].createdAt").exists()
                .jsonPath("$.data.messages[1].id").isEqualTo("message-2")
                .jsonPath("$.data.messages[1].role").isEqualTo("assistant")
                .jsonPath("$.data.nextCursor").isEqualTo("cursor-2")
                .jsonPath("$.data.userId").doesNotExist()
                .jsonPath("$.data.messages[0].messageId").doesNotExist()
                .jsonPath("$.data.messages[0].status").doesNotExist()
                .jsonPath("$.data.messages[0].sequenceNo").doesNotExist()
                .jsonPath("$.success").doesNotExist()
                .jsonPath("$.message").doesNotExist();

        assertThat(service.messagesUserId).isEqualTo("codex-bruno-test");
        assertThat(service.messagesConversationId).isEqualTo("conversation-1");
        assertThat(service.messagesLimit).isEqualTo(50);
        assertThat(service.messagesCursor).isEqualTo("cursor-1");
    }

    @Test
    void deleteConversationReturnsDeletedSummary() {
        StubConversationService service = new StubConversationService(
                null,
                null,
                null,
                new RagConversationSummaryView(
                        "conversation-1",
                        "待删除会话",
                        CREATED_AT,
                        OffsetDateTime.parse("2026-06-17T00:03:00Z")
                )
        );
        WebTestClient client = WebTestClient
                .bindToController(new RagConversationController(service))
                .build();

        client.delete()
                .uri(uriBuilder -> uriBuilder
                        .path("/public/rag/conversations/conversation-1")
                        .queryParam("userId", "codex-bruno-test")
                        .build())
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.data.conversationId").isEqualTo("conversation-1")
                .jsonPath("$.data.title").isEqualTo("待删除会话")
                .jsonPath("$.data.createdAt").exists()
                .jsonPath("$.data.updatedAt").exists()
                .jsonPath("$.data.status").doesNotExist()
                .jsonPath("$.success").doesNotExist()
                .jsonPath("$.message").doesNotExist();

        assertThat(service.deleteUserId).isEqualTo("codex-bruno-test");
        assertThat(service.deleteConversationId).isEqualTo("conversation-1");
    }

    private static final class StubConversationService extends RagConversationService {

        private final RagConversationListView listResponse;
        private final RagConversationSummaryView updateResponse;
        private final RagConversationMessagesView messagesResponse;
        private String listUserId;
        private Integer listLimit;
        private String listCursor;
        private String updateConversationId;
        private RagConversationUpdateRequest updateRequest;
        private String messagesUserId;
        private String messagesConversationId;
        private Integer messagesLimit;
        private String messagesCursor;
        private final RagConversationSummaryView deleteResponse;
        private String deleteUserId;
        private String deleteConversationId;

        private StubConversationService(RagConversationListView listResponse) {
            this(listResponse, null, null, null);
        }

        private StubConversationService(RagConversationSummaryView updateResponse) {
            this(null, updateResponse, null, null);
        }

        private StubConversationService(RagConversationMessagesView messagesResponse) {
            this(null, null, messagesResponse, null);
        }

        private StubConversationService(
                RagConversationListView listResponse,
                RagConversationSummaryView updateResponse,
                RagConversationMessagesView messagesResponse,
                RagConversationSummaryView deleteResponse
        ) {
            super(null, null, null, null, null);
            this.listResponse = listResponse;
            this.updateResponse = updateResponse;
            this.messagesResponse = messagesResponse;
            this.deleteResponse = deleteResponse;
        }

        @Override
        public RagConversationListView listConversations(String userId, Integer limit, String cursor) {
            this.listUserId = userId;
            this.listLimit = limit;
            this.listCursor = cursor;
            return listResponse;
        }

        @Override
        public RagConversationSummaryView updateConversation(
                String conversationId,
                RagConversationUpdateRequest request
        ) {
            this.updateConversationId = conversationId;
            this.updateRequest = request;
            return updateResponse;
        }

        @Override
        public RagConversationMessagesView getMessages(
                String userId,
                String conversationId,
                Integer limit,
                String cursor
        ) {
            this.messagesUserId = userId;
            this.messagesConversationId = conversationId;
            this.messagesLimit = limit;
            this.messagesCursor = cursor;
            return messagesResponse;
        }

        @Override
        public RagConversationSummaryView deleteConversation(String userId, String conversationId) {
            this.deleteUserId = userId;
            this.deleteConversationId = conversationId;
            return deleteResponse;
        }
    }
}
