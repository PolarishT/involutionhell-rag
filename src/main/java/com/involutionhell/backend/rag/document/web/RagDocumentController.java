package com.involutionhell.backend.rag.document.web;

import com.involutionhell.backend.rag.common.api.ApiResponse;
import com.involutionhell.backend.rag.document.api.*;
import com.involutionhell.backend.rag.shared.markdown.MarkdownDocumentParser;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Positive;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.http.MediaType;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.util.StringUtils;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@RestController
@Validated
@RequestMapping(value = "/public/rag/documents", produces = MediaType.APPLICATION_JSON_VALUE)
public class RagDocumentController {

    private static final Logger log = LoggerFactory.getLogger(RagDocumentController.class);
    private static final int MAX_FILE_SIZE = 10 * 1024 * 1024;
    private static final Pattern FIRST_HEADING_PATTERN = Pattern.compile("(?m)^#\\s+(.+?)\\s*$");
    private final DocumentCommandFacade documentCommandFacade;
    private final DocumentQueryFacade documentQueryFacade;
    private final MarkdownDocumentParser markdownDocumentParser;

    public RagDocumentController(
            DocumentCommandFacade documentCommandFacade,
            DocumentQueryFacade documentQueryFacade,
            MarkdownDocumentParser markdownDocumentParser
    ) {
        this.documentCommandFacade = documentCommandFacade;
        this.documentQueryFacade = documentQueryFacade;
        this.markdownDocumentParser = markdownDocumentParser;
    }

    @PostMapping(value = "/create", consumes = MediaType.APPLICATION_JSON_VALUE)
    public Mono<ApiResponse<RagDocumentView>> createDocument(
            @Valid @RequestBody RagDocumentCreateRequest request
    ) {
        return Mono.fromCallable(() ->
                ApiResponse.ok(
                        "文档已接收，开始索引",
                        documentCommandFacade.createDocument(request)
                )
        );
    }

    @PostMapping(value = "/create", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public Mono<ApiResponse<RagDocumentView>> createDocument(
            @RequestPart("file") FilePart file
    ) {
        String originalFilename = resolveOriginalFilename(file);
        MediaType contentType = file.headers().getContentType();

        String rejectionReason =
                classifyMarkdownRejection(originalFilename, contentType);

        if (rejectionReason != null) {
            log.warn(
                    "非法文件上传尝试: filename={}, contentType={}, rejectReason={}",
                    originalFilename,
                    contentType,
                    rejectionReason
            );

            return Mono.error(new IllegalArgumentException(
                    "安全拦截：只允许上传 Markdown 文件（.md、.markdown 或 .mdx）"
            ));
        }

        return DataBufferUtils.join(file.content(), MAX_FILE_SIZE)
                .map(dataBuffer -> {
                    try {
                        int contentLength = dataBuffer.readableByteCount();
                        byte[] bytes = new byte[contentLength];
                        dataBuffer.read(bytes);

                        return new UploadedMarkdown(
                                new String(bytes, StandardCharsets.UTF_8),
                                contentLength
                        );
                    } finally {
                        DataBufferUtils.release(dataBuffer);
                    }
                })
                .switchIfEmpty(Mono.error(
                        new IllegalArgumentException("上传文件内容为空")
                )).flatMap(uploadedMarkdown -> {
                    String content = uploadedMarkdown.content();

                    if (!StringUtils.hasText(content)) {
                        return Mono.error(
                                new IllegalArgumentException("上传文件内容不能为空")
                        );
                    }

                    if (content.indexOf('\0') >= 0) {
                        return Mono.error(new IllegalArgumentException(
                                "安全拦截：文件内容包含非法二进制空字符"
                        ));
                    }

                    InferredUploadMetadata inferred =
                            inferUploadMetadata(content, originalFilename);

                    Map<String, Object> uploadMetadata = new LinkedHashMap<>();
                    uploadMetadata.put("originalFilename", originalFilename);

                    if (contentType != null) {
                        uploadMetadata.put("contentType", contentType.toString());
                    }

                    uploadMetadata.put("uploadMode", "multipart");
                    uploadMetadata.put("metadataInferred", true);
                    uploadMetadata.put(
                            "uploadTime",
                            ZonedDateTime.now(ZoneId.of("Australia/Sydney"))
                    );

                    logUpload(
                            originalFilename,
                            uploadedMarkdown.contentLength(),
                            inferred
                    );

                    RagDocumentCreateRequest request =
                            new RagDocumentCreateRequest(
                                    inferred.sourceType(),
                                    inferred.sourceUri(),
                                    inferred.externalRef(),
                                    inferred.title(),
                                    content,
                                    uploadMetadata
                            );

                    return Mono.fromCallable(() ->
                                    documentCommandFacade.createDocument(request)
                            )
                            .subscribeOn(Schedulers.boundedElastic());
                })
                .map(view -> ApiResponse.ok(
                        "文档已接收，开始索引",
                        view
                ));
    }


    @GetMapping(value = "/get/{documentId}")
    public ApiResponse<RagDocumentView> getDocument(@PathVariable @Positive Long documentId) {
        return ApiResponse.ok(documentQueryFacade.getDocument(documentId));
    }

    @PutMapping(value = "/update/{documentId}", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ApiResponse<RagDocumentView> updateDocument(
            @PathVariable @Positive Long documentId,
            @Valid @RequestBody RagDocumentUpdateRequest request
    ) {
        return ApiResponse.ok("文档已更新，开始重建索引", documentCommandFacade.updateDocument(documentId, request));
    }

    @PostMapping(value = "/reindex/{documentId}")
    public ApiResponse<RagDocumentView> reindexDocument(@PathVariable @Positive Long documentId) {
        return ApiResponse.ok("文档已重新提交索引", documentCommandFacade.reindexDocument(documentId));
    }

    @DeleteMapping(value = "/del/{documentId}")
    public ApiResponse<Void> deleteDocument(@PathVariable @Positive Long documentId) {
        documentCommandFacade.deleteDocument(documentId);
        return ApiResponse.okMessage("文档已进入删除流程");
    }

    private String resolveOriginalFilename(FilePart file) {
        Objects.requireNonNull(file, "file 不能为 null");

        String cleanedFilename = StringUtils.cleanPath(file.filename());

        if (!StringUtils.hasText(cleanedFilename)) {
            return "uploaded-document.md";
        }

        cleanedFilename = cleanedFilename.trim();

        int lastSlash = Math.max(
                cleanedFilename.lastIndexOf('/'),
                cleanedFilename.lastIndexOf('\\')
        );

        String filename = lastSlash >= 0
                ? cleanedFilename.substring(lastSlash + 1)
                : cleanedFilename;

        if (!StringUtils.hasText(filename)) {
            return "uploaded-document.md";
        }

        return filename;
    }

    /**
     * 严格验证是否为 Markdown 文件，返回拒绝原因（null 表示通过）。
     * 用作日志可观测字段，便于 on-call 区分误传 vs 恶意上传。
     */
    private String classifyMarkdownRejection(
            String filename,
            @Nullable MediaType contentType
    ) {
        if (!StringUtils.hasText(filename)) {
            return "MISSING_FILENAME";
        }

        String lowerFilename = filename.toLowerCase(Locale.ROOT);

        boolean validExtension =
                lowerFilename.endsWith(".md")
                        || lowerFilename.endsWith(".markdown")
                        || lowerFilename.endsWith(".mdx");

        if (!validExtension) {
            return "INVALID_EXTENSION";
        }

        if (contentType != null) {
            String lowerContentType =
                    contentType.toString().toLowerCase(Locale.ROOT);

            if (lowerContentType.contains("html")
                    || lowerContentType.contains("javascript")
                    || lowerContentType.contains("shell")
                    || lowerContentType.contains("executable")) {
                return "DANGEROUS_CONTENT_TYPE";
            }
        }

        return null;
    }

    private InferredUploadMetadata inferUploadMetadata(String content, String originalFilename) {
        MarkdownDocumentParser.MarkdownDocument markdownDocument = markdownDocumentParser.parse(content);
        Map<String, Object> frontmatter = markdownDocument.frontmatter();
        String title = firstText(frontmatter, "title", "name")
                .or(() -> firstHeading(markdownDocument.bodyContent()))
                .orElseGet(() -> titleFromFilename(originalFilename));
        String sourceType = firstText(frontmatter, "sourceType", "source_type", "type", "format")
                .orElse("MARKDOWN");
        String sourceUri = firstText(frontmatter, "sourceUri", "source_uri", "uri", "url", "canonicalUrl", "canonical_url", "path", "file")
                .orElseGet(() -> "upload://" + originalFilename);
        String externalRef = firstText(frontmatter, "externalRef", "external_ref", "ref", "id", "slug")
                .orElse(null);
        return new InferredUploadMetadata(sourceType, sourceUri, externalRef, title);
    }

    private java.util.Optional<String> firstText(Map<String, Object> values, String... keys) {
        for (String key : keys) {
            Object value = values.get(key);
            if (value instanceof String stringValue && StringUtils.hasText(stringValue)) {
                return java.util.Optional.of(stringValue.trim());
            }
            if (value != null && !(value instanceof Map<?, ?>) && !(value instanceof Iterable<?>)) {
                String text = value.toString();
                if (StringUtils.hasText(text)) {
                    return java.util.Optional.of(text.trim());
                }
            }
        }
        return java.util.Optional.empty();
    }

    private java.util.Optional<String> firstHeading(String bodyContent) {
        if (!StringUtils.hasText(bodyContent)) {
            return java.util.Optional.empty();
        }
        Matcher matcher = FIRST_HEADING_PATTERN.matcher(bodyContent);
        if (!matcher.find()) {
            return java.util.Optional.empty();
        }
        return java.util.Optional.of(matcher.group(1).trim());
    }

    private String titleFromFilename(String originalFilename) {
        if (!StringUtils.hasText(originalFilename)) {
            return "uploaded-document";
        }
        String title = originalFilename;
        int dotIndex = title.lastIndexOf('.');
        if (dotIndex > 0) {
            title = title.substring(0, dotIndex);
        }
        return title.replace('-', ' ').replace('_', ' ').trim();
    }

    private void logUpload(String originalFilename, int contentLength, InferredUploadMetadata inferred) {
        log.info(
                "RAG upload received: filename={}, sourceType={}, sourceUri={}, title={}, size={}",
                originalFilename,
                inferred.sourceType(),
                inferred.sourceUri(),
                inferred.title(),
                contentLength
        );
    }

    private record UploadedMarkdown(
            String content,
            int contentLength
    ) {
    }

    private record InferredUploadMetadata(
            String sourceType,
            String sourceUri,
            String externalRef,
            String title
    ) {
    }
}
