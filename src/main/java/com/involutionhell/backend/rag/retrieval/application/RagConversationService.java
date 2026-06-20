package com.involutionhell.backend.rag.retrieval.application;

import com.involutionhell.backend.rag.retrieval.api.RagContextView;
import com.involutionhell.backend.rag.retrieval.api.RagConversationListView;
import com.involutionhell.backend.rag.retrieval.api.RagConversationMessage;
import com.involutionhell.backend.rag.retrieval.api.RagConversationMessageView;
import com.involutionhell.backend.rag.retrieval.api.RagConversationMessagesView;
import com.involutionhell.backend.rag.retrieval.api.RagConversationSummaryView;
import com.involutionhell.backend.rag.retrieval.api.RagConversationUpdateRequest;
import com.involutionhell.backend.rag.retrieval.api.RagResponseNoticeView;
import com.involutionhell.backend.rag.retrieval.persistence.RagAskRunRepository;
import com.involutionhell.backend.rag.retrieval.persistence.RagAskRunRecord;
import com.involutionhell.backend.rag.retrieval.persistence.RagConversationCursor;
import com.involutionhell.backend.rag.retrieval.persistence.RagConversationMessageCursor;
import com.involutionhell.backend.rag.retrieval.persistence.RagConversationMessagePage;
import com.involutionhell.backend.rag.retrieval.persistence.RagConversationMessageRecord;
import com.involutionhell.backend.rag.retrieval.persistence.RagConversationMessageRepository;
import com.involutionhell.backend.rag.retrieval.persistence.RagConversationPage;
import com.involutionhell.backend.rag.retrieval.persistence.RagConversationRecord;
import com.involutionhell.backend.rag.retrieval.persistence.RagConversationRepository;
import com.involutionhell.backend.rag.retrieval.persistence.RagStaleAskRunRecord;
import com.involutionhell.backend.rag.retrieval.persistence.RagUserRepository;
import com.involutionhell.backend.rag.shared.metadata.RagSearchFilter;
import com.involutionhell.backend.rag.shared.support.RagJsonCodec;
import com.involutionhell.backend.rag.shared.support.RagLogHelper;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;
import org.springframework.web.server.ResponseStatusException;

@Service
public class RagConversationService {

    private static final int HISTORY_LIMIT = 10;
    private static final int DEFAULT_PAGE_LIMIT = 20;
    private static final int MAX_PAGE_LIMIT = 100;
    private static final int GENERATED_TITLE_MAX_LENGTH = 60;
    private static final String ASK_FAILED_MESSAGE_PREFIX = "本轮问答失败，请重试。原因：";
    private static final String STALE_ASK_RECOVERY_CODE = "StaleRunningAskRecovered";
    private static final String STALE_ASK_RECOVERY_MESSAGE = "问答流异常中断且失败标记未及时落库，系统已自动恢复失败状态。";

    private final RagUserRepository userRepository;
    private final RagConversationRepository conversationRepository;
    private final RagConversationMessageRepository messageRepository;
    private final RagAskRunRepository askRunRepository;
    private final RagJsonCodec jsonCodec;

    public RagConversationService(
            RagUserRepository userRepository,
            RagConversationRepository conversationRepository,
            RagConversationMessageRepository messageRepository,
            RagAskRunRepository askRunRepository,
            RagJsonCodec jsonCodec
    ) {
        this.userRepository = userRepository;
        this.conversationRepository = conversationRepository;
        this.messageRepository = messageRepository;
        this.askRunRepository = askRunRepository;
        this.jsonCodec = jsonCodec;
    }

    @Transactional(rollbackFor = Exception.class)
    public AskConversationState beginAsk(
            String runId,
            String correlationId,
            String userId,
            String conversationId,
            String question,
            String requestId,
            Integer topK,
            RagSearchFilter filter
    ) {
        requireText(userId, "userId 不能为空");
        requireText(question, "问题不能为空");
        String normalizedRequestId = normalizeRequestId(requestId);
        userRepository.upsertSeen(userId);
        RagConversationRecord conversation = getOrCreateOwnedConversation(userId, conversationId, question);
        conversationRepository.lockById(conversation.id());
        conversation = requireOwnedConversation(userId, conversation.conversationId(), true);
        if (normalizedRequestId != null) {
            var existingRun = askRunRepository.findByRequestId(userId, conversation.id(), normalizedRequestId);
            if (existingRun.isPresent()) {
                return existingAskState(conversation, question, existingRun.get());
            }
        }
        RagConversationMessageRecord userMessage = messageRepository.append(
                conversation.id(),
                "user",
                question,
                "SUCCEEDED",
                correlationId
        );
        List<RagConversationMessage> history = messageRepository.findRecentByConversationId(
                        conversation.id(),
                        HISTORY_LIMIT + 1
                ).stream()
                .filter(message -> !message.id().equals(userMessage.id()))
                .filter(message -> "SUCCEEDED".equals(message.status()))
                .limit(HISTORY_LIMIT)
                .map(message -> new RagConversationMessage(message.role(), message.content()))
                .toList();
        RagConversationMessageRecord assistantMessage = messageRepository.append(
                conversation.id(),
                "assistant",
                "",
                "STREAMING",
                correlationId
        );
        askRunRepository.createRunning(
                runId,
                correlationId,
                userId,
                conversation.id(),
                userMessage.id(),
                assistantMessage.id(),
                normalizedRequestId,
                question,
                topK,
                toFilterMap(filter)
        );
        conversationRepository.refreshStats(conversation.id());
        return new AskConversationState(
                conversation,
                userMessage,
                assistantMessage,
                history,
                runId,
                correlationId,
                normalizedRequestId,
                "RUNNING",
                null,
                null,
                false
        );
    }

    @Transactional(rollbackFor = Exception.class)
    public RagConversationMessageRecord completeAsk(
            AskConversationState state,
            String answer,
            String retrievalQuestion,
            List<String> retrievalQueries,
            List<RagContextView> contexts,
            List<RagResponseNoticeView> notices,
            boolean generatedByModel,
            boolean degraded,
            String correlationId
    ) {
        conversationRepository.lockById(state.conversation().id());
        RagConversationMessageRecord assistantMessage = messageRepository.updateContentAndStatus(
                state.assistantMessage().id(),
                answer == null ? "" : answer,
                "SUCCEEDED"
        );
        conversationRepository.refreshStats(state.conversation().id());
        askRunRepository.markSucceeded(
                state.runId(),
                assistantMessage.id(),
                retrievalQuestion,
                retrievalQueries,
                contexts,
                notices,
                generatedByModel,
                degraded
        );
        return assistantMessage;
    }

    @Transactional(rollbackFor = Exception.class)
    public RagConversationMessageRecord streamAssistantAnswer(AskConversationState state, String answer) {
        return messageRepository.updateContentAndStatus(
                state.assistantMessage().id(),
                answer == null ? "" : answer,
                "STREAMING"
        );
    }

    @Transactional(rollbackFor = Exception.class)
    public void failAsk(AskConversationState state, Throwable exception, List<RagResponseNoticeView> notices) {
        if (state == null) {
            return;
        }
        conversationRepository.lockById(state.conversation().id());
        RagConversationMessageRecord assistantMessage = messageRepository.findById(state.assistantMessage().id());
        String content = StringUtils.hasText(assistantMessage.content())
                ? assistantMessage.content()
                : failureMessage(exception);
        messageRepository.updateContentAndStatus(
                state.assistantMessage().id(),
                content,
                "FAILED"
        );
        conversationRepository.refreshStats(state.conversation().id());
        askRunRepository.markFailed(
                state.runId(),
                exception.getClass().getSimpleName(),
                RagLogHelper.errorSummary(exception),
                notices
        );
    }

    @Transactional(readOnly = true)
    public List<RagStaleAskRunRecord> findStaleRunningAsks(OffsetDateTime startedBefore, int limit) {
        return askRunRepository.findStaleRunning(startedBefore, limit);
    }

    @Transactional(rollbackFor = Exception.class)
    public void recoverStaleRunningAsk(RagStaleAskRunRecord staleRun) {
        if (staleRun == null) {
            return;
        }
        conversationRepository.lockById(staleRun.conversationId());
        if (staleRun.assistantMessageId() != null) {
            RagConversationMessageRecord assistantMessage = messageRepository.findById(staleRun.assistantMessageId());
            String content = StringUtils.hasText(assistantMessage.content())
                    ? assistantMessage.content()
                    : ASK_FAILED_MESSAGE_PREFIX + STALE_ASK_RECOVERY_MESSAGE;
            messageRepository.updateContentAndStatus(staleRun.assistantMessageId(), content, "FAILED");
        }
        conversationRepository.refreshStats(staleRun.conversationId());
        askRunRepository.markFailed(
                staleRun.runId(),
                STALE_ASK_RECOVERY_CODE,
                STALE_ASK_RECOVERY_MESSAGE,
                List.of(new RagResponseNoticeView("ask", "recovered_failure", STALE_ASK_RECOVERY_MESSAGE))
        );
    }

    public RagConversationListView listConversations(String userId, Integer limit, String cursor) {
        requireText(userId, "userId 不能为空");
        int pageLimit = normalizeLimit(limit);
        RagConversationPage page = conversationRepository.findByUserId(userId, pageLimit, decodeCursor(cursor));
        return new RagConversationListView(
                page.items().stream().map(this::toSummaryView).toList(),
                encodeCursor(page.nextCursor())
        );
    }

    public RagConversationMessagesView getMessages(String userId, String conversationId) {
        return getMessages(userId, conversationId, null, null);
    }

    public RagConversationMessagesView getMessages(String userId, String conversationId, Integer limit, String cursor) {
        requireText(userId, "userId 不能为空");
        requireText(conversationId, "conversationId 不能为空");
        int pageLimit = normalizeLimit(limit);
        RagConversationRecord conversation = requireOwnedConversation(userId, conversationId, false);
        RagConversationMessagePage page = messageRepository.findByConversationId(
                conversation.id(),
                pageLimit,
                decodeMessageCursor(cursor)
        );
        List<RagConversationMessageView> messages = page.items()
                .stream()
                .map(this::toMessageView)
                .toList();
        return new RagConversationMessagesView(
                conversation.conversationId(),
                messages,
                encodeMessageCursor(page.nextCursor())
        );
    }

    @Transactional(rollbackFor = Exception.class)
    public RagConversationSummaryView updateConversation(String conversationId, RagConversationUpdateRequest request) {
        requireText(conversationId, "conversationId 不能为空");
        requireText(request.userId(), "userId 不能为空");
        String status = normalizeStatus(request.status());
        String title = request.title() == null ? null : request.title().trim();
        if (title != null && title.length() > 200) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "title 长度不能超过 200");
        }
        return toSummaryView(conversationRepository.update(request.userId(), conversationId, title, status));
    }

    @Transactional(rollbackFor = Exception.class)
    public RagConversationSummaryView deleteConversation(String userId, String conversationId) {
        requireText(userId, "userId 不能为空");
        requireText(conversationId, "conversationId 不能为空");
        RagConversationRecord conversation = conversationRepository.findByUserIdAndConversationId(userId, conversationId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "会话不存在"));
        conversationRepository.lockById(conversation.id());
        conversation = conversationRepository.findByUserIdAndConversationId(userId, conversationId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "会话不存在"));
        if ("DELETED".equals(conversation.status())) {
            return toSummaryView(conversation);
        }
        return toSummaryView(conversationRepository.update(userId, conversationId, null, "DELETED"));
    }

    private RagConversationRecord requireOwnedConversation(String userId, String conversationId, boolean detectConflict) {
        RagConversationRecord conversation = conversationRepository.findByConversationId(conversationId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "会话不存在"));
        if (!conversation.userId().equals(userId)) {
            HttpStatus status = detectConflict ? HttpStatus.CONFLICT : HttpStatus.NOT_FOUND;
            throw new ResponseStatusException(status, detectConflict ? "conversationId 已被其他 userId 使用" : "会话不存在");
        }
        if ("DELETED".equals(conversation.status())) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "会话不存在");
        }
        return conversation;
    }

    private RagConversationRecord getOrCreateOwnedConversation(String userId, String conversationId, String question) {
        String normalizedConversationId = normalizeConversationId(conversationId);
        if (normalizedConversationId == null) {
            return createGeneratedConversation(userId, question);
        }
        return conversationRepository.findByConversationId(normalizedConversationId)
                .map(conversation -> requireUsableConversation(userId, conversation, true))
                .orElseGet(() -> createProvidedConversation(userId, normalizedConversationId, question));
    }

    private RagConversationRecord createGeneratedConversation(String userId, String question) {
        for (int attempt = 0; attempt < 3; attempt++) {
            try {
                String generatedConversationId = "conv:" + UUID.randomUUID();
                return conversationRepository.create(userId, generatedConversationId, titleFromQuestion(question));
            } catch (DuplicateKeyException ignored) {
                // UUID 冲突极低；重试可覆盖并发或测试中的人为碰撞。
            }
        }
        throw new ResponseStatusException(HttpStatus.CONFLICT, "conversationId 生成冲突，请重试");
    }

    private RagConversationRecord createProvidedConversation(String userId, String conversationId, String question) {
        try {
            return conversationRepository.create(userId, conversationId, titleFromQuestion(question));
        } catch (DuplicateKeyException ignored) {
            return requireOwnedConversation(userId, conversationId, true);
        }
    }

    private AskConversationState existingAskState(
            RagConversationRecord conversation,
            String question,
            RagAskRunRecord existingRun
    ) {
        if (!existingRun.question().equals(question)) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "requestId 已用于不同问题");
        }
        RagConversationMessageRecord userMessage = messageRepository.findById(existingRun.userMessageId());
        RagConversationMessageRecord assistantMessage = messageRepository.findById(existingRun.assistantMessageId());
        return new AskConversationState(
                conversation,
                userMessage,
                assistantMessage,
                List.of(),
                existingRun.runId(),
                existingRun.correlationId(),
                existingRun.requestId(),
                existingRun.status(),
                existingRun.errorCode(),
                existingRun.errorMessage(),
                true
        );
    }

    private int normalizeLimit(Integer limit) {
        if (limit == null) {
            return DEFAULT_PAGE_LIMIT;
        }
        if (limit < 1) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "limit 最小为 1");
        }
        return Math.min(limit, MAX_PAGE_LIMIT);
    }

    private String normalizeStatus(String status) {
        if (!StringUtils.hasText(status)) {
            return null;
        }
        String normalized = status.trim().toUpperCase(Locale.ROOT);
        if (!List.of("ACTIVE", "ARCHIVED", "DELETED").contains(normalized)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "status 只能是 ACTIVE、ARCHIVED 或 DELETED");
        }
        return normalized;
    }

    private String normalizeConversationId(String conversationId) {
        if (!StringUtils.hasText(conversationId)) {
            return null;
        }
        String normalized = conversationId.trim();
        if (normalized.length() > 128) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "conversationId 长度不能超过 128");
        }
        return normalized;
    }

    private RagConversationRecord requireUsableConversation(
            String userId,
            RagConversationRecord conversation,
            boolean detectConflict
    ) {
        if (!conversation.userId().equals(userId)) {
            HttpStatus status = detectConflict ? HttpStatus.CONFLICT : HttpStatus.NOT_FOUND;
            throw new ResponseStatusException(status, detectConflict ? "conversationId 已被其他 userId 使用" : "会话不存在");
        }
        if ("DELETED".equals(conversation.status())) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "会话不存在");
        }
        return conversation;
    }

    private String titleFromQuestion(String question) {
        String normalized = question == null ? "" : question.trim().replaceAll("\\s+", " ");
        if (normalized.isEmpty()) {
            return null;
        }
        return normalized.length() <= GENERATED_TITLE_MAX_LENGTH
                ? normalized
                : normalized.substring(0, GENERATED_TITLE_MAX_LENGTH);
    }

    private String failureMessage(Throwable exception) {
        String summary = RagLogHelper.errorSummary(exception);
        return ASK_FAILED_MESSAGE_PREFIX + (StringUtils.hasText(summary) ? summary : "未知错误");
    }

    private String normalizeRequestId(String requestId) {
        if (!StringUtils.hasText(requestId)) {
            return null;
        }
        String normalized = requestId.trim();
        if (normalized.length() > 128) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "requestId 长度不能超过 128");
        }
        return normalized;
    }

    private RagConversationSummaryView toSummaryView(RagConversationRecord conversation) {
        return new RagConversationSummaryView(
                conversation.conversationId(),
                conversation.title(),
                conversation.createdAt(),
                conversation.updatedAt()
        );
    }

    private RagConversationMessageView toMessageView(RagConversationMessageRecord message) {
        return new RagConversationMessageView(
                message.messageId(),
                message.role(),
                message.content(),
                message.createdAt()
        );
    }

    private Map<String, Object> toFilterMap(RagSearchFilter filter) {
        Map<String, Object> filters = new LinkedHashMap<>();
        if (filter == null) {
            return filters;
        }
        filters.put("sourceUriPrefix", filter.sourceUriPrefix());
        filters.put("tags", filter.tags());
        filters.put("headingPathContains", filter.headingPathContains());
        return filters;
    }

    private RagConversationCursor decodeCursor(String cursor) {
        if (!StringUtils.hasText(cursor)) {
            return null;
        }
        try {
            String json = new String(Base64.getUrlDecoder().decode(cursor), StandardCharsets.UTF_8);
            Map<String, Object> value = jsonCodec.readMap(json);
            Object id = value.get("id");
            if (id == null) {
                throw new IllegalArgumentException("cursor missing id");
            }
            Object lastMessageAt = value.get("lastMessageAt");
            return new RagConversationCursor(
                    lastMessageAt == null ? null : OffsetDateTime.parse(String.valueOf(lastMessageAt)),
                    Long.valueOf(String.valueOf(id))
            );
        } catch (Exception exception) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "cursor 不合法", exception);
        }
    }

    private RagConversationMessageCursor decodeMessageCursor(String cursor) {
        if (!StringUtils.hasText(cursor)) {
            return null;
        }
        try {
            String json = new String(Base64.getUrlDecoder().decode(cursor), StandardCharsets.UTF_8);
            Map<String, Object> value = jsonCodec.readMap(json);
            Object sequenceNo = value.get("sequenceNo");
            Object id = value.get("id");
            if (sequenceNo == null || id == null) {
                throw new IllegalArgumentException("cursor missing sequenceNo or id");
            }
            return new RagConversationMessageCursor(
                    Integer.parseInt(String.valueOf(sequenceNo)),
                    Long.valueOf(String.valueOf(id))
            );
        } catch (Exception exception) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "cursor 不合法", exception);
        }
    }

    private String encodeCursor(RagConversationCursor cursor) {
        if (cursor == null) {
            return null;
        }
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("lastMessageAt", cursor.lastMessageAt() == null ? null : cursor.lastMessageAt().toString());
        value.put("id", cursor.id());
        return Base64.getUrlEncoder()
                .withoutPadding()
                .encodeToString(jsonCodec.write(value).getBytes(StandardCharsets.UTF_8));
    }

    private String encodeMessageCursor(RagConversationMessageCursor cursor) {
        if (cursor == null) {
            return null;
        }
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("sequenceNo", cursor.sequenceNo());
        value.put("id", cursor.id());
        return Base64.getUrlEncoder()
                .withoutPadding()
                .encodeToString(jsonCodec.write(value).getBytes(StandardCharsets.UTF_8));
    }

    private void requireText(String value, String message) {
        if (!StringUtils.hasText(value)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, message);
        }
    }

    public record AskConversationState(
            RagConversationRecord conversation,
            RagConversationMessageRecord userMessage,
            RagConversationMessageRecord assistantMessage,
            List<RagConversationMessage> history,
            String runId,
            String correlationId,
            String requestId,
            String runStatus,
            String errorCode,
            String errorMessage,
            boolean existing
    ) {
    }
}
