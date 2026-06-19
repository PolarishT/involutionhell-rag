package com.involutionhell.backend.rag.indexing.persistence.jdbc;

import com.involutionhell.backend.rag.indexing.model.RagChunkDraft;
import com.involutionhell.backend.rag.indexing.persistence.RagChunkRecord;
import com.involutionhell.backend.rag.indexing.persistence.RagChunkRepository;
import com.involutionhell.backend.rag.indexing.persistence.RagChunkSearchRecord;
import com.involutionhell.backend.rag.shared.persistence.RagDatabaseDialect;
import com.involutionhell.backend.rag.shared.persistence.RagJdbcJson;
import com.involutionhell.backend.rag.shared.persistence.RagJdbcRows;
import com.involutionhell.backend.rag.shared.support.RagJsonCodec;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
public class JdbcRagChunkRepository implements RagChunkRepository {

    private static final String CHUNK_COLUMNS = """
            id, document_id, index_generation, chunk_index, chunk_text, chunk_hash,
            char_count, token_count, vector_id, metadata, created_at, updated_at
            """;

    private static final String SEARCH_COLUMNS = """
            c.id AS chunk_id,
            c.document_id,
            d.title,
            d.source_type,
            d.source_uri,
            d.external_ref,
            c.index_generation,
            c.chunk_index,
            c.chunk_text,
            c.vector_id,
            c.metadata
            """;

    private static final String SEARCH_FROM = """
            FROM rag_chunks c
            INNER JOIN rag_documents d ON d.id = c.document_id
            """;

    private static final String SEARCHABLE_CONDITION = """
            d.indexed_generation IS NOT NULL
            AND d.status <> 'DELETING'
            AND c.index_generation = d.indexed_generation
            """;

    private static final String FTS_VECTOR = """
            (
                setweight(to_tsvector('simple', COALESCE(c.metadata->>'headingPathText', '')), 'A')
                || setweight(to_tsvector('simple', COALESCE(d.title, '')), 'B')
                || setweight(to_tsvector('simple', COALESCE(c.chunk_text, '')), 'C')
            )
            """;

    private final JdbcClient jdbcClient;
    private final RagJsonCodec jsonCodec;
    private final RagDatabaseDialect databaseDialect;

    public JdbcRagChunkRepository(
            JdbcClient jdbcClient,
            RagJsonCodec jsonCodec,
            RagDatabaseDialect databaseDialect
    ) {
        this.jdbcClient = jdbcClient;
        this.jsonCodec = jsonCodec;
        this.databaseDialect = databaseDialect;
    }

    @Override
    @Transactional
    public List<RagChunkRecord> saveAll(Long documentId, List<RagChunkDraft> chunks) {
        if (chunks == null || chunks.isEmpty()) {
            return List.of();
        }
        String metadataParameter = RagJdbcJson.parameter(databaseDialect, "metadata");
        String sql = """
                INSERT INTO rag_chunks (
                    document_id, index_generation, chunk_index, chunk_text, chunk_hash,
                    char_count, token_count, vector_id, metadata
                ) VALUES (
                    :documentId, :indexGeneration, :chunkIndex, :chunkText, :chunkHash,
                    :charCount, :tokenCount, :vectorId, %s
                )
                """.formatted(metadataParameter);
        for (RagChunkDraft chunk : chunks) {
            jdbcClient.sql(sql)
                    .param("documentId", documentId)
                    .param("indexGeneration", chunk.indexGeneration())
                    .param("chunkIndex", chunk.chunkIndex())
                    .param("chunkText", chunk.chunkText())
                    .param("chunkHash", chunk.chunkHash())
                    .param("charCount", chunk.charCount())
                    .param("tokenCount", chunk.tokenCount())
                    .param("vectorId", chunk.vectorId())
                    .param("metadata", jsonCodec.write(chunk.metadata() == null ? Map.of() : chunk.metadata()))
                    .update();
        }
        return reloadInsertedChunks(documentId, chunks);
    }

    @Override
    public List<String> findVectorIdsByDocumentId(Long documentId) {
        return jdbcClient.sql("""
                SELECT DISTINCT vector_id
                FROM rag_chunks
                WHERE document_id = :documentId AND vector_id IS NOT NULL
                """)
                .param("documentId", documentId)
                .query(String.class)
                .list();
    }

    @Override
    public List<String> findVectorIdsByDocumentIdAndGeneration(Long documentId, Long indexGeneration) {
        return jdbcClient.sql("""
                SELECT DISTINCT vector_id
                FROM rag_chunks
                WHERE document_id = :documentId
                  AND index_generation = :indexGeneration
                  AND vector_id IS NOT NULL
                """)
                .param("documentId", documentId)
                .param("indexGeneration", indexGeneration)
                .query(String.class)
                .list();
    }

    @Override
    public List<RagChunkRecord> findByDocumentIdAndGeneration(Long documentId, Long indexGeneration) {
        return jdbcClient.sql("SELECT " + CHUNK_COLUMNS + """
                 FROM rag_chunks
                 WHERE document_id = :documentId AND index_generation = :indexGeneration
                 ORDER BY chunk_index ASC
                """)
                .param("documentId", documentId)
                .param("indexGeneration", indexGeneration)
                .query(this::mapChunk)
                .list();
    }

    @Override
    public List<String> findVectorIdsByDocumentIdExceptGeneration(Long documentId, Long indexGeneration) {
        return jdbcClient.sql("""
                SELECT DISTINCT vector_id
                FROM rag_chunks
                WHERE document_id = :documentId
                  AND index_generation <> :indexGeneration
                  AND vector_id IS NOT NULL
                """)
                .param("documentId", documentId)
                .param("indexGeneration", indexGeneration)
                .query(String.class)
                .list();
    }

    @Override
    public Integer findMaxChunkIndexByDocumentIdAndGeneration(Long documentId, Long indexGeneration) {
        return jdbcClient.sql("""
                SELECT MAX(chunk_index)
                FROM rag_chunks
                WHERE document_id = :documentId AND index_generation = :indexGeneration
                """)
                .param("documentId", documentId)
                .param("indexGeneration", indexGeneration)
                .query(Integer.class)
                .optional()
                .orElse(null);
    }

    @Override
    public Integer findMaxChunkIndexByDocumentIdExceptGeneration(Long documentId, Long indexGeneration) {
        return jdbcClient.sql("""
                SELECT MAX(chunk_index)
                FROM rag_chunks
                WHERE document_id = :documentId AND index_generation <> :indexGeneration
                """)
                .param("documentId", documentId)
                .param("indexGeneration", indexGeneration)
                .query(Integer.class)
                .optional()
                .orElse(null);
    }

    @Override
    public void deleteByDocumentId(Long documentId) {
        delete("document_id = :documentId", documentId, null);
    }

    @Override
    public void deleteByDocumentIdAndGeneration(Long documentId, Long indexGeneration) {
        delete("document_id = :documentId AND index_generation = :indexGeneration", documentId, indexGeneration);
    }

    @Override
    public void deleteByDocumentIdExceptGeneration(Long documentId, Long indexGeneration) {
        delete("document_id = :documentId AND index_generation <> :indexGeneration", documentId, indexGeneration);
    }

    @Override
    public List<RagChunkSearchRecord> findKeywordCandidates(Set<String> tokens, int limit) {
        List<String> normalizedTokens = tokens == null ? List.of() : tokens.stream()
                .filter(token -> token != null && !token.isBlank())
                .map(token -> token.toLowerCase(Locale.ROOT))
                .limit(8)
                .toList();
        if (normalizedTokens.isEmpty() || limit <= 0) {
            return List.of();
        }
        if (databaseDialect.isPostgreSql()) {
            return findKeywordCandidatesPostgreSql(normalizedTokens, limit, databaseDialect.isPgTrgmEnabled());
        }
        return findKeywordCandidatesFallback(normalizedTokens, limit);
    }

    @Override
    public List<RagChunkSearchRecord> findActiveChunksByDocumentIdAndRange(
            Long documentId,
            int startChunkIndex,
            int endChunkIndex
    ) {
        return jdbcClient.sql("SELECT " + SEARCH_COLUMNS + SEARCH_FROM + """
                 WHERE %s
                   AND c.document_id = :documentId
                   AND c.chunk_index BETWEEN :startChunkIndex AND :endChunkIndex
                 ORDER BY c.chunk_index ASC
                """.formatted(SEARCHABLE_CONDITION))
                .param("documentId", documentId)
                .param("startChunkIndex", startChunkIndex)
                .param("endChunkIndex", endChunkIndex)
                .query(this::mapSearchRecord)
                .list();
    }

    @Override
    public List<RagChunkSearchRecord> findSearchableByVectorIds(List<String> vectorIds) {
        if (vectorIds == null || vectorIds.isEmpty()) {
            return List.of();
        }
        List<RagChunkSearchRecord> rows = jdbcClient.sql("SELECT " + SEARCH_COLUMNS + SEARCH_FROM + """
                 WHERE %s AND c.vector_id IN (:vectorIds)
                """.formatted(SEARCHABLE_CONDITION))
                .param("vectorIds", vectorIds)
                .query(this::mapSearchRecord)
                .list();

        Map<String, RagChunkSearchRecord> byVectorId = rows.stream()
                .collect(Collectors.toMap(RagChunkSearchRecord::vectorId, row -> row, (left, right) -> left));
        List<RagChunkSearchRecord> ordered = new ArrayList<>();
        for (String vectorId : vectorIds) {
            RagChunkSearchRecord row = byVectorId.get(vectorId);
            if (row != null) {
                ordered.add(row);
            }
        }
        return ordered;
    }

    private List<RagChunkSearchRecord> findKeywordCandidatesPostgreSql(
            List<String> normalizedTokens,
            int limit,
            boolean trigramEnabled
    ) {
        String searchText = String.join(" ", normalizedTokens);
        String likePattern = likePattern(searchText);
        String ftsScore = "ts_rank_cd(" + FTS_VECTOR + ", plainto_tsquery('simple', :searchText))";
        String trigramScore = trigramEnabled ? """
                GREATEST(
                    similarity(LOWER(COALESCE(c.metadata->>'headingPathText', '')), :searchText),
                    similarity(LOWER(COALESCE(d.title, '')), :searchText),
                    similarity(LOWER(COALESCE(c.chunk_text, '')), :searchText)
                )
                """ : "0.0";
        String trigramMatch = trigramEnabled ? """
                 OR LOWER(COALESCE(c.metadata->>'headingPathText', '')) % :searchText
                 OR LOWER(COALESCE(d.title, '')) % :searchText
                 OR LOWER(COALESCE(c.chunk_text, '')) % :searchText
                """ : "";
        String sql = "SELECT " + SEARCH_COLUMNS + ", "
                + ftsScore + " AS fts_score, "
                + trigramScore + " AS trigram_score, ("
                + ftsScore + " * 10.0 + " + trigramScore + " * 3.0) AS rank_score "
                + SEARCH_FROM + """
                 WHERE %s
                   AND (
                       %s @@ plainto_tsquery('simple', :searchText)
                       %s
                       OR LOWER(COALESCE(c.metadata->>'headingPathText', '')) LIKE :likePattern ESCAPE '\\'
                       OR LOWER(COALESCE(d.title, '')) LIKE :likePattern ESCAPE '\\'
                       OR LOWER(COALESCE(c.chunk_text, '')) LIKE :likePattern ESCAPE '\\'
                   )
                 ORDER BY rank_score DESC, c.document_id ASC, c.chunk_index ASC
                 LIMIT :limit
                """.formatted(SEARCHABLE_CONDITION, FTS_VECTOR, trigramMatch);
        return jdbcClient.sql(sql)
                .param("searchText", searchText)
                .param("likePattern", likePattern)
                .param("limit", limit)
                .query(this::mapSearchRecord)
                .list();
    }

    private List<RagChunkSearchRecord> findKeywordCandidatesFallback(List<String> normalizedTokens, int limit) {
        String metadataText = "LOWER(CAST(c.metadata AS VARCHAR))";
        List<String> scoreTerms = new ArrayList<>();
        List<String> matchClauses = new ArrayList<>();
        Map<String, Object> parameters = new LinkedHashMap<>();
        for (int index = 0; index < normalizedTokens.size(); index++) {
            String parameter = "token" + index;
            String placeholder = ":" + parameter;
            scoreTerms.add("""
                    (CASE WHEN LOWER(c.chunk_text) LIKE %s ESCAPE '\\' THEN 1 ELSE 0 END
                     + CASE WHEN LOWER(COALESCE(d.title, '')) LIKE %s ESCAPE '\\' THEN 1 ELSE 0 END
                     + CASE WHEN %s LIKE %s ESCAPE '\\' THEN 1 ELSE 0 END)
                    """.formatted(placeholder, placeholder, metadataText, placeholder));
            matchClauses.add("""
                    (LOWER(c.chunk_text) LIKE %s ESCAPE '\\'
                     OR LOWER(COALESCE(d.title, '')) LIKE %s ESCAPE '\\'
                     OR %s LIKE %s ESCAPE '\\')
                    """.formatted(placeholder, placeholder, metadataText, placeholder));
            parameters.put(parameter, likePattern(normalizedTokens.get(index)));
        }
        parameters.put("limit", limit);
        String sql = "SELECT " + SEARCH_COLUMNS + ", ("
                + String.join(" + ", scoreTerms) + ") AS candidate_score "
                + SEARCH_FROM + """
                 WHERE %s AND (%s)
                 ORDER BY candidate_score DESC, c.document_id ASC, c.chunk_index ASC
                 LIMIT :limit
                """.formatted(SEARCHABLE_CONDITION, String.join(" OR ", matchClauses));
        return jdbcClient.sql(sql)
                .params(parameters)
                .query(this::mapSearchRecord)
                .list();
    }

    private void delete(String condition, Long documentId, Long indexGeneration) {
        JdbcClient.StatementSpec statement = jdbcClient.sql("DELETE FROM rag_chunks WHERE " + condition)
                .param("documentId", documentId);
        if (indexGeneration != null) {
            statement.param("indexGeneration", indexGeneration);
        }
        statement.update();
    }

    private List<RagChunkRecord> reloadInsertedChunks(Long documentId, List<RagChunkDraft> chunks) {
        Long indexGeneration = chunks.getFirst().indexGeneration();
        List<RagChunkRecord> inserted = findByDocumentIdAndGeneration(documentId, indexGeneration);
        Set<Integer> expectedChunkIndexes = chunks.stream()
                .map(RagChunkDraft::chunkIndex)
                .collect(Collectors.toCollection(LinkedHashSet::new));
        List<RagChunkRecord> filtered = inserted.stream()
                .filter(record -> expectedChunkIndexes.contains(record.chunkIndex()))
                .toList();
        if (filtered.size() != chunks.size()) {
            throw new IllegalStateException(
                    "批量创建 RAG 切片后回查数量不一致: expected=" + chunks.size() + ", actual=" + filtered.size()
            );
        }
        return filtered;
    }

    private RagChunkRecord mapChunk(ResultSet resultSet, int rowNumber) throws SQLException {
        return new RagChunkRecord(
                RagJdbcRows.longValue(resultSet, "id"),
                RagJdbcRows.longValue(resultSet, "document_id"),
                RagJdbcRows.longValue(resultSet, "index_generation"),
                RagJdbcRows.intValue(resultSet, "chunk_index"),
                RagJdbcRows.string(resultSet, "chunk_text"),
                RagJdbcRows.string(resultSet, "chunk_hash"),
                RagJdbcRows.intValue(resultSet, "char_count"),
                RagJdbcRows.intValue(resultSet, "token_count"),
                RagJdbcRows.string(resultSet, "vector_id"),
                RagJdbcRows.jsonMap(jsonCodec, resultSet, "metadata"),
                RagJdbcRows.offsetDateTime(resultSet, "created_at"),
                RagJdbcRows.offsetDateTime(resultSet, "updated_at")
        );
    }

    private RagChunkSearchRecord mapSearchRecord(ResultSet resultSet, int rowNumber) throws SQLException {
        return new RagChunkSearchRecord(
                RagJdbcRows.longValue(resultSet, "chunk_id"),
                RagJdbcRows.longValue(resultSet, "document_id"),
                RagJdbcRows.string(resultSet, "title"),
                RagJdbcRows.string(resultSet, "source_type"),
                RagJdbcRows.string(resultSet, "source_uri"),
                RagJdbcRows.string(resultSet, "external_ref"),
                RagJdbcRows.longValue(resultSet, "index_generation"),
                RagJdbcRows.intValue(resultSet, "chunk_index"),
                RagJdbcRows.string(resultSet, "chunk_text"),
                RagJdbcRows.string(resultSet, "vector_id"),
                RagJdbcRows.jsonMap(jsonCodec, resultSet, "metadata")
        );
    }

    private String likePattern(String token) {
        return "%" + token
                .replace("\\", "\\\\")
                .replace("%", "\\%")
                .replace("_", "\\_") + "%";
    }
}
