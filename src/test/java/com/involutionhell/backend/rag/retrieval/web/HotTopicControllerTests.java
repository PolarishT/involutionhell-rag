package com.involutionhell.backend.rag.retrieval.web;

import com.involutionhell.backend.rag.infrastructure.web.RagExceptionHandler;
import com.involutionhell.backend.rag.retrieval.application.HotTopicQueryService;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;

class HotTopicControllerTests {

    @Test
    void returnsDefaultHotTopicsAsJson() {
        WebTestClient client = client();

        client.get()
                .uri("/public/rag/hot-topics")
                .exchange()
                .expectStatus().isOk()
                .expectHeader().contentTypeCompatibleWith(MediaType.APPLICATION_JSON)
                .expectBody()
                .jsonPath("$.data.hotTopics").isArray()
                .jsonPath("$.data.hotTopics.length()").isEqualTo(4)
                .jsonPath("$.data.hotTopics[0].id").isEqualTo("unsw-2027-qs-ranking")
                .jsonPath("$.data.hotTopics[0].label").isEqualTo("UNSW 2027 QS Ranking")
                .jsonPath("$.data.hotTopics[0].prompt").isEqualTo("介绍一下 UNSW 2027 QS Ranking")
                .jsonPath("$.data.hotTopics[3].id").isEqualTo("ai-design-paradigm");
    }

    @Test
    void honorsLimitParameter() {
        client().get()
                .uri("/public/rag/hot-topics?limit=2")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.data.hotTopics.length()").isEqualTo(2)
                .jsonPath("$.data.hotTopics[1].id").isEqualTo("andrew-ng-machine-learning");
    }

    @Test
    void rejectsLimitOutsideSupportedRange() {
        client().get()
                .uri("/public/rag/hot-topics?limit=6")
                .exchange()
                .expectStatus().isBadRequest()
                .expectHeader().contentTypeCompatibleWith(MediaType.APPLICATION_JSON)
                .expectBody()
                .jsonPath("$.message").isEqualTo("limit 必须在 1 到 5 之间");
    }

    private WebTestClient client() {
        return WebTestClient
                .bindToController(new HotTopicController(new HotTopicQueryService()))
                .controllerAdvice(new RagExceptionHandler())
                .build();
    }
}
