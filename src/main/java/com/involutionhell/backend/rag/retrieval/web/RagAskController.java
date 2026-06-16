package com.involutionhell.backend.rag.retrieval.web;

import com.involutionhell.backend.rag.retrieval.api.RagAskStreamEvent;
import com.involutionhell.backend.rag.retrieval.api.RagAskFacade;
import com.involutionhell.backend.rag.retrieval.api.RagAskRequest;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Flux;

@RestController
@RequestMapping("/public/rag")
@Validated
public class RagAskController {

    private final RagAskFacade ragAskFacade;

    public RagAskController(RagAskFacade ragAskFacade) {
        this.ragAskFacade = ragAskFacade;
    }

    @GetMapping(value = "/ask", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<Object>> ask(
            @RequestParam @NotBlank(message = "userId 不能为空") String userId,
            @RequestParam(required = false) String conversationId,
            @RequestParam @NotBlank(message = "问题不能为空") @Size(max = 4096, message = "问题长度不能超过 4096") String question,
            @RequestParam(required = false) @Min(value = 1, message = "topK 最小为 1") @Max(value = 10, message = "topK 最大为 10") Integer topK,
            @RequestParam(required = false) String sourceUriPrefix,
            @RequestParam(required = false) List<String> tags,
            @RequestParam(required = false) String headingPathContains,
            @RequestParam(required = false) String requestId
    ) {
        validateRequestParams(userId, question, topK);
        RagAskRequest request = new RagAskRequest(
                userId,
                conversationId,
                question,
                topK,
                sourceUriPrefix,
                tags,
                headingPathContains,
                List.of(),
                requestId
        );
        return  Flux.defer(() -> ragAskFacade.askStream(request))
                .map(this::toServerSentEvent);
    }

    private void validateRequestParams(String userId, String question, Integer topK) {
        if (userId == null || userId.isBlank()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "userId 不能为空");
        }
        if (question == null || question.isBlank()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "问题不能为空");
        }
        if (question.length() > 4096) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "问题长度不能超过 4096");
        }
        if (topK != null && (topK < 1 || topK > 10)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "topK 必须在 1 到 10 之间");
        }
    }

    private ServerSentEvent<Object> toServerSentEvent(RagAskStreamEvent event) {
        return ServerSentEvent.builder(event.data())
                .id(event.id())
                .event(event.event())
                .build();
    }
}
