package com.involutionhell.backend.rag.retrieval.api;

import java.util.List;

public record HotTopicsView(List<HotTopicItemView> hotTopics) {

    public HotTopicsView {
        hotTopics = hotTopics == null ? List.of() : List.copyOf(hotTopics);
    }
}
