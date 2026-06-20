package com.involutionhell.backend.rag.retrieval.application;

import com.involutionhell.backend.rag.retrieval.api.HotTopicItemView;
import com.involutionhell.backend.rag.retrieval.api.HotTopicsView;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.springframework.stereotype.Service;

@Service
public class HotTopicQueryService {

    static final int DEFAULT_LIMIT = 4;
    static final int MIN_LIMIT = 1;
    static final int MAX_LIMIT = 5;

    private static final List<HotTopicDefinition> DEFAULT_HOT_TOPICS = List.of(
            new HotTopicDefinition(
                    "unsw-2027-qs-ranking",
                    "UNSW 2027 QS Ranking",
                    "介绍一下 UNSW 2027 QS Ranking",
                    10,
                    true
            ),
            new HotTopicDefinition(
                    "andrew-ng-machine-learning",
                    "吴恩达机器学习",
                    "介绍一下吴恩达机器学习课程",
                    20,
                    true
            ),
            new HotTopicDefinition(
                    "shell-quick-start",
                    "shell 快速入门",
                    "介绍一下 shell 快速入门",
                    30,
                    true
            ),
            new HotTopicDefinition(
                    "ai-design-paradigm",
                    "快来发现 AI 时代的新设计范式。",
                    "介绍一下 AI 时代的新设计范式",
                    40,
                    true
            )
    );

    private final List<HotTopicDefinition> hotTopics;

    public HotTopicQueryService() {
        this(DEFAULT_HOT_TOPICS);
    }

    HotTopicQueryService(List<HotTopicDefinition> hotTopics) {
        this.hotTopics = validateAndCopy(hotTopics);
    }

    public HotTopicsView getHotTopics(Integer limit) {
        int resolvedLimit = limit == null ? DEFAULT_LIMIT : limit;
        validateLimit(resolvedLimit);

        List<HotTopicItemView> items = hotTopics.stream()
                .filter(HotTopicDefinition::enabled)
                .sorted(Comparator.comparingInt(HotTopicDefinition::sortOrder))
                .limit(resolvedLimit)
                .map(topic -> new HotTopicItemView(topic.id(), topic.label(), topic.prompt()))
                .toList();

        return new HotTopicsView(items);
    }

    private static void validateLimit(int limit) {
        if (limit < MIN_LIMIT || limit > MAX_LIMIT) {
            throw new IllegalArgumentException("limit 必须在 1 到 5 之间");
        }
    }

    private static List<HotTopicDefinition> validateAndCopy(List<HotTopicDefinition> hotTopics) {
        List<HotTopicDefinition> topics = List.copyOf(hotTopics);
        Set<String> ids = new HashSet<>();

        for (HotTopicDefinition topic : topics) {
            if (!ids.add(topic.id())) {
                throw new IllegalArgumentException("热门话题 id 必须全局唯一: " + topic.id());
            }
        }

        return topics;
    }

    record HotTopicDefinition(
            String id,
            String label,
            String prompt,
            int sortOrder,
            boolean enabled
    ) {

        HotTopicDefinition {
            requireNotBlank(id, "id");
            requireNotBlank(label, "label");
            requireNotBlank(prompt, "prompt");
        }

        private static void requireNotBlank(String value, String field) {
            if (value == null || value.isBlank()) {
                throw new IllegalArgumentException("热门话题 " + field + " 不能为空");
            }
        }
    }
}
