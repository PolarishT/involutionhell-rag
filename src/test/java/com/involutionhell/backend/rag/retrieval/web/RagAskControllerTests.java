package com.involutionhell.backend.rag.retrieval.web;

import com.involutionhell.backend.rag.retrieval.api.RagAnswerDeltaView;
import com.involutionhell.backend.rag.retrieval.api.RagAskCompletedView;
import com.involutionhell.backend.rag.retrieval.api.RagAskFacade;
import com.involutionhell.backend.rag.retrieval.api.RagAskRequest;
import com.involutionhell.backend.rag.retrieval.api.RagAskStartedView;
import com.involutionhell.backend.rag.retrieval.api.RagAskStreamEvent;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Flux;

import static org.assertj.core.api.Assertions.assertThat;

class RagAskControllerTests {

    @Test
    void askEndpointStreamsServerSentEvents() {
        WebTestClient client = WebTestClient
                .bindToController(new RagAskController(new FixedAskFacade()))
                .build();

        List<ServerSentEvent<String>> events = client.get()
                .uri(uriBuilder -> uriBuilder
                        .path("/public/rag/ask")
                        .queryParam("userId", "user-001")
                        .queryParam("conversationId", "conv-001")
                        .queryParam("question", "怎么使用 RAG?")
                        .queryParam("topK", 3)
                        .build())
                .accept(MediaType.TEXT_EVENT_STREAM)
                .exchange()
                .expectStatus().isOk()
                .expectHeader().contentTypeCompatibleWith(MediaType.TEXT_EVENT_STREAM)
                .returnResult(new ParameterizedTypeReference<ServerSentEvent<String>>() {
                })
                .getResponseBody()
                .collectList()
                .block();

        assertThat(events).isNotNull();
        assertThat(events).extracting(ServerSentEvent::event)
                .containsExactly("started", "answer_delta", "completed");
    }

    @Test
    void askEndpointBindsRepeatedTags() {
        CapturingAskFacade facade = new CapturingAskFacade();
        WebTestClient client = WebTestClient
                .bindToController(new RagAskController(facade))
                .build();

        client.get()
                .uri(uriBuilder -> uriBuilder
                        .path("/public/rag/ask")
                        .queryParam("userId", "user-001")
                        .queryParam("question", "过滤标签是否生效?")
                        .queryParam("tags", "rag")
                        .queryParam("tags", "java")
                        .queryParam("sourceUriPrefix", "s3://docs/")
                        .queryParam("headingPathContains", "API")
                        .queryParam("requestId", "request-001")
                        .build())
                .accept(MediaType.TEXT_EVENT_STREAM)
                .exchange()
                .expectStatus().isOk()
                .expectHeader().contentTypeCompatibleWith(MediaType.TEXT_EVENT_STREAM);

        RagAskRequest request = facade.request.get();
        assertThat(request).isNotNull();
        assertThat(request.conversationId()).isNull();
        assertThat(request.tags()).containsExactly("rag", "java");
        assertThat(request.sourceUriPrefix()).isEqualTo("s3://docs/");
        assertThat(request.headingPathContains()).isEqualTo("API");
        assertThat(request.requestId()).isEqualTo("request-001");
        assertThat(request.history()).isEmpty();
    }

    @Test
    void askEndpointRejectsBlankQuestion() {
        WebTestClient client = WebTestClient
                .bindToController(new RagAskController(new FixedAskFacade()))
                .build();

        client.get()
                .uri(uriBuilder -> uriBuilder
                        .path("/public/rag/ask")
                        .queryParam("userId", "user-001")
                        .queryParam("conversationId", "conv-001")
                        .queryParam("question", "")
                        .queryParam("topK", 3)
                        .build())
                .accept(MediaType.TEXT_EVENT_STREAM)
                .exchange()
                .expectStatus().isBadRequest();
    }

    @Test
    void askEndpointRejectsMissingUserId() {
        WebTestClient client = WebTestClient
                .bindToController(new RagAskController(new FixedAskFacade()))
                .build();

        client.get()
                .uri(uriBuilder -> uriBuilder
                        .path("/public/rag/ask")
                        .queryParam("question", "问题")
                        .queryParam("topK", 3)
                        .build())
                .accept(MediaType.TEXT_EVENT_STREAM)
                .exchange()
                .expectStatus().isBadRequest();
    }

    @Test
    void askEndpointRejectsTopKOutOfRange() {
        WebTestClient client = WebTestClient
                .bindToController(new RagAskController(new FixedAskFacade()))
                .build();

        client.get()
                .uri(uriBuilder -> uriBuilder
                        .path("/public/rag/ask")
                        .queryParam("userId", "user-001")
                        .queryParam("question", "问题")
                        .queryParam("topK", 11)
                        .build())
                .exchange()
                .expectStatus().isBadRequest();
    }

    private static class FixedAskFacade implements RagAskFacade {

        @Override
        public Flux<RagAskStreamEvent> askStream(RagAskRequest request) {
            String correlationId = "ask:test";
            return Flux.just(
                    new RagAskStreamEvent(
                            "1",
                            "started",
                            correlationId,
                            new RagAskStartedView(
                                    correlationId,
                                    "conv-001",
                                    request.question(),
                                    request.topK() == null ? 3 : request.topK()
                            )
                    ),
                    new RagAskStreamEvent(
                            "2",
                            "answer_delta",
                            correlationId,
                            new RagAnswerDeltaView("ok")
                    ),
                    new RagAskStreamEvent(
                            "3",
                            "completed",
                            correlationId,
                            new RagAskCompletedView(false, false, List.of(), 0)
                    )
            );
        }
    }

    private static final class CapturingAskFacade extends FixedAskFacade {

        private final AtomicReference<RagAskRequest> request = new AtomicReference<>();

        @Override
        public Flux<RagAskStreamEvent> askStream(RagAskRequest request) {
            this.request.set(request);
            return super.askStream(request);
        }
    }
}
