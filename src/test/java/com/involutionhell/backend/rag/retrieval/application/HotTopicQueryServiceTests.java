package com.involutionhell.backend.rag.retrieval.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.involutionhell.backend.rag.retrieval.api.HotTopicItemView;
import java.util.List;
import org.junit.jupiter.api.Test;

class HotTopicQueryServiceTests {

    @Test
    void returnsEnabledTopicsSortedBySortOrderWithDefaultLimit() {
        HotTopicQueryService service = new HotTopicQueryService(List.of(
                topic("fifth", 50, true),
                topic("disabled", 5, false),
                topic("second", 20, true),
                topic("first", 10, true),
                topic("fourth", 40, true),
                topic("third", 30, true)
        ));

        List<HotTopicItemView> hotTopics = service.getHotTopics(null).hotTopics();

        assertThat(hotTopics)
                .extracting(HotTopicItemView::id)
                .containsExactly("first", "second", "third", "fourth");
    }

    @Test
    void honorsRequestedLimitAndNeverReturnsNull() {
        HotTopicQueryService service = new HotTopicQueryService(List.of(
                topic("second", 20, true),
                topic("first", 10, true)
        ));

        assertThat(service.getHotTopics(1).hotTopics())
                .extracting(HotTopicItemView::id)
                .containsExactly("first");
        assertThat(new HotTopicQueryService(List.of()).getHotTopics(null).hotTopics())
                .isEmpty();
    }

    @Test
    void rejectsLimitOutsideSupportedRange() {
        HotTopicQueryService service = new HotTopicQueryService();

        assertThatThrownBy(() -> service.getHotTopics(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("limit 必须在 1 到 5 之间");
        assertThatThrownBy(() -> service.getHotTopics(6))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("limit 必须在 1 到 5 之间");
    }

    private HotTopicQueryService.HotTopicDefinition topic(
            String id,
            int sortOrder,
            boolean enabled
    ) {
        return new HotTopicQueryService.HotTopicDefinition(
                id,
                "label-" + id,
                "prompt-" + id,
                sortOrder,
                enabled
        );
    }
}
