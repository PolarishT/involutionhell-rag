package com.involutionhell.backend.rag.retrieval.web;

import com.involutionhell.backend.rag.common.api.DataResponse;
import com.involutionhell.backend.rag.retrieval.api.HotTopicsView;
import com.involutionhell.backend.rag.retrieval.application.HotTopicQueryService;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
@Validated
@RequestMapping(value = "/public/rag", produces = MediaType.APPLICATION_JSON_VALUE)
public class HotTopicController {

    private final HotTopicQueryService hotTopicQueryService;

    public HotTopicController(HotTopicQueryService hotTopicQueryService) {
        this.hotTopicQueryService = hotTopicQueryService;
    }

    @GetMapping("/hot-topics")
    public DataResponse<HotTopicsView> getHotTopics(
            @RequestParam(required = false)
            @Min(value = 1, message = "limit 最小为 1")
            @Max(value = 5, message = "limit 最大为 5")
            Integer limit
    ) {
        if (limit != null && (limit < 1 || limit > 5)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "limit 必须在 1 到 5 之间");
        }

        return DataResponse.of(hotTopicQueryService.getHotTopics(limit));
    }
}
