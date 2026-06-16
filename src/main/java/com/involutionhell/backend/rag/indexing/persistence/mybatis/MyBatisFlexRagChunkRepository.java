package com.involutionhell.backend.rag.indexing.persistence.mybatis;

import com.involutionhell.backend.rag.indexing.model.RagChunkDraft;
import com.involutionhell.backend.rag.indexing.persistence.RagChunkRecord;
import com.involutionhell.backend.rag.indexing.persistence.RagChunkRepository;
import com.involutionhell.backend.rag.indexing.persistence.RagChunkSearchRecord;
import com.involutionhell.backend.rag.shared.persistence.RagDatabaseDialect;
import com.involutionhell.backend.rag.shared.persistence.RagFlexJson;
import com.involutionhell.backend.rag.shared.persistence.RagFlexRows;
import com.involutionhell.backend.rag.shared.support.RagJsonCodec;
import com.mybatisflex.core.query.QueryChain;
import com.mybatisflex.core.query.QueryColumn;
import com.mybatisflex.core.query.RawQueryOrderBy;
import com.mybatisflex.core.query.RawQueryColumn;
import com.mybatisflex.core.row.DbChain;
import com.mybatisflex.core.row.Row;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
public class MyBatisFlexRagChunkRepository implements RagChunkRepository {

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

    private final RagChunkMapper chunkMapper;
    private final RagJsonCodec jsonCodec;
    private final RagDatabaseDialect databaseDialect;

    public MyBatisFlexRagChunkRepository(
            RagChunkMapper chunkMapper,
            RagJsonCodec jsonCodec,
            RagDatabaseDialect databaseDialect
    ) {
        this.chunkMapper = chunkMapper;
        this.jsonCodec = jsonCodec;
        this.databaseDialect = databaseDialect;
    }

    @Override
    @Transactional
    public List<RagChunkRecord> saveAll(Long documentId, List<RagChunkDraft> chunks) {
        if (chunks == null || chunks.isEmpty()) {
            return List.of();
        }
        for (RagChunkDraft chunk : chunks) {
            DbChain insert = DbChain.table("rag_chunks")
                    .set("document_id", documentId)
                    .set("index_generation", chunk.indexGeneration())
                    .set("chunk_index", chunk.chunkIndex())
                    .set("chunk_text", chunk.chunkText())
                    .set("chunk_hash", chunk.chunkHash())
                    .set("char_count", chunk.charCount())
                    .set("token_count", chunk.tokenCount())
                    .set("vector_id", chunk.vectorId());
            RagFlexJson.setJson(
                            insert,
                            databaseDialect,
                            "metadata",
                            jsonCodec.write(chunk.metadata() == null ? Map.of() : chunk.metadata())
                    )
                    .save();
        }
        return reloadInsertedChunks(documentId, chunks);
    }

    @Override
    public List<String> findVectorIdsByDocumentId(Long documentId) {
        return DbChain.table("rag_chunks")
                .select(raw("DISTINCT vector_id"))
                .where("document_id = ? AND vector_id IS NOT NULL", documentId)
                .objListAs(String.class);
    }

    @Override
    public List<String> findVectorIdsByDocumentIdAndGeneration(Long documentId, Long indexGeneration) {
        return DbChain.table("rag_chunks")
                .select(raw("DISTINCT vector_id"))
                .where("document_id = ? AND index_generation = ? AND vector_id IS NOT NULL",
                        documentId, indexGeneration)
                .objListAs(String.class);
    }

    @Override
    public List<RagChunkRecord> findByDocumentIdAndGeneration(Long documentId, Long indexGeneration) {
        return QueryChain.of(chunkMapper)
                .where("document_id = ? AND index_generation = ?", documentId, indexGeneration)
                .orderBy("chunk_index ASC")
                .list()
                .stream()
                .map(this::toRecord)
                .toList();
    }

    @Override
    public List<String> findVectorIdsByDocumentIdExceptGeneration(Long documentId, Long indexGeneration) {
        return DbChain.table("rag_chunks")
                .select(raw("DISTINCT vector_id"))
                .where("document_id = ? AND index_generation <> ? AND vector_id IS NOT NULL",
                        documentId, indexGeneration)
                .objListAs(String.class);
    }

    @Override
    public Integer findMaxChunkIndexByDocumentIdAndGeneration(Long documentId, Long indexGeneration) {
        return DbChain.table("rag_chunks")
                .select(raw("MAX(chunk_index)"))
                .where("document_id = ? AND index_generation = ?", documentId, indexGeneration)
                .objAsOpt(Integer.class)
                .orElse(null);
    }

    @Override
    public Integer findMaxChunkIndexByDocumentIdExceptGeneration(Long documentId, Long indexGeneration) {
        return DbChain.table("rag_chunks")
                .select(raw("MAX(chunk_index)"))
                .where("document_id = ? AND index_generation <> ?", documentId, indexGeneration)
                .objAsOpt(Integer.class)
                .orElse(null);
    }

    @Override
    public void deleteByDocumentId(Long documentId) {
        DbChain.table("rag_chunks").where("document_id = ?", documentId).remove();
    }

    @Override
    public void deleteByDocumentIdAndGeneration(Long documentId, Long indexGeneration) {
        DbChain.table("rag_chunks")
                .where("document_id = ? AND index_generation = ?", documentId, indexGeneration)
                .remove();
    }

    @Override
    public void deleteByDocumentIdExceptGeneration(Long documentId, Long indexGeneration) {
        DbChain.table("rag_chunks")
                .where("document_id = ? AND index_generation <> ?", documentId, indexGeneration)
                .remove();
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
        return searchableBase()
                .where(SEARCHABLE_CONDITION)
                .and("c.document_id = ? AND c.chunk_index BETWEEN ? AND ?",
                        documentId, startChunkIndex, endChunkIndex)
                .orderBy("c.chunk_index ASC")
                .list()
                .stream()
                .map(this::toSearchRecord)
                .toList();
    }

    @Override
    public List<RagChunkSearchRecord> findSearchableByVectorIds(List<String> vectorIds) {
        if (vectorIds == null || vectorIds.isEmpty()) {
            return List.of();
        }
        List<RagChunkSearchRecord> rows = searchableBase()
                .where(SEARCHABLE_CONDITION)
                .and("c.vector_id IN (" + placeholders(vectorIds.size()) + ")", vectorIds.toArray())
                .list()
                .stream()
                .map(this::toSearchRecord)
                .toList();

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

        String ftsExpr = "ts_rank_cd(" + FTS_VECTOR + ", plainto_tsquery('simple', ?))";

        String trigramExpr;
        if (trigramEnabled) {
            trigramExpr = """
                GREATEST(
                    similarity(LOWER(COALESCE(c.metadata->>'headingPathText', '')), ?),
                    similarity(LOWER(COALESCE(d.title, '')), ?),
                    similarity(LOWER(COALESCE(c.chunk_text, '')), ?)
                )
                """;
        } else {
            trigramExpr = "0.0";
        }

        List<QueryColumn> selectColumns = new ArrayList<>(List.of(searchSelectColumns()));

        // fts_score
        selectColumns.add(raw(
                ftsExpr + " AS fts_score",
                searchText
        ));

        // trigram_score
        if (trigramEnabled) {
            selectColumns.add(raw(
                    trigramExpr + " AS trigram_score",
                    searchText, searchText, searchText
            ));
        } else {
            selectColumns.add(raw("0.0 AS trigram_score"));
        }

        // rank_score：不要引用 fts_score / trigram_score alias，直接重复表达式
        if (trigramEnabled) {
            selectColumns.add(raw(
                    "(" + ftsExpr + " * 10.0 + " + trigramExpr + " * 3.0) AS rank_score",
                    searchText,
                    searchText, searchText, searchText
            ));
        } else {
            selectColumns.add(raw(
                    "(" + ftsExpr + " * 10.0 + 0.0 * 3.0) AS rank_score",
                    searchText
            ));
        }

        List<Object> conditionArgs = new ArrayList<>();
        conditionArgs.add(searchText);

        StringBuilder condition = new StringBuilder("(")
                .append(FTS_VECTOR)
                .append(" @@ plainto_tsquery('simple', ?)");

        if (trigramEnabled) {
            condition.append("""
                OR LOWER(COALESCE(c.metadata->>'headingPathText', '')) % ?
                OR LOWER(COALESCE(d.title, '')) % ?
                OR LOWER(COALESCE(c.chunk_text, '')) % ?
                """);
            conditionArgs.add(searchText);
            conditionArgs.add(searchText);
            conditionArgs.add(searchText);
        }

        condition.append("""
            OR LOWER(COALESCE(c.metadata->>'headingPathText', '')) LIKE ? ESCAPE '\\'
            OR LOWER(COALESCE(d.title, '')) LIKE ? ESCAPE '\\'
            OR LOWER(COALESCE(c.chunk_text, '')) LIKE ? ESCAPE '\\'
            )
            """);

        conditionArgs.add(likePattern);
        conditionArgs.add(likePattern);
        conditionArgs.add(likePattern);

        return searchableBase(selectColumns.toArray(QueryColumn[]::new))
                .where(SEARCHABLE_CONDITION)
                .and(condition.toString(), conditionArgs.toArray())
                .orderBy(
                        new RawQueryOrderBy("rank_score DESC", false),
                        new RawQueryOrderBy("c.document_id ASC"),
                        new RawQueryOrderBy("c.chunk_index ASC")
                )
                .limit(limit)
                .list()
                .stream()
                .map(this::toSearchRecord)
                .toList();
    }

    private List<RagChunkSearchRecord> findKeywordCandidatesFallback(List<String> normalizedTokens, int limit) {
        String metadataText = "LOWER(CAST(c.metadata AS TEXT))";
        List<String> scoreTerms = new ArrayList<>();
        List<String> matchClauses = new ArrayList<>();
        List<Object> args = new ArrayList<>();

        for (String token : normalizedTokens) {
            String pattern = likePattern(token);
            scoreTerms.add("""
                    (CASE WHEN LOWER(c.chunk_text) LIKE ? ESCAPE '\\' THEN 1 ELSE 0 END
                     + CASE WHEN LOWER(COALESCE(d.title, '')) LIKE ? ESCAPE '\\' THEN 1 ELSE 0 END
                     + CASE WHEN %s LIKE ? ESCAPE '\\' THEN 1 ELSE 0 END)
                    """.formatted(metadataText));
            args.add(pattern);
            args.add(pattern);
            args.add(pattern);
        }

        for (String token : normalizedTokens) {
            String pattern = likePattern(token);
            matchClauses.add("""
                    (LOWER(c.chunk_text) LIKE ? ESCAPE '\\'
                     OR LOWER(COALESCE(d.title, '')) LIKE ? ESCAPE '\\'
                     OR %s LIKE ? ESCAPE '\\')
                    """.formatted(metadataText));
            args.add(pattern);
            args.add(pattern);
            args.add(pattern);
        }

        QueryColumn[] selectColumns = appendSearchColumn(raw(
                "(" + String.join(" + ", scoreTerms) + ") AS candidate_score"
        ));
        return searchableBase(selectColumns)
                .where(SEARCHABLE_CONDITION)
                .and("(" + String.join(" OR ", matchClauses) + ")", args.toArray())
                .orderBy("candidate_score DESC", "c.document_id ASC", "c.chunk_index ASC")
                .limit(limit)
                .list()
                .stream()
                .map(this::toSearchRecord)
                .toList();
    }

    private DbChain searchableBase() {
        return searchableBase(searchSelectColumns());
    }

    private DbChain searchableBase(QueryColumn... selectColumns) {
        return DbChain.table("rag_chunks")
                .as("c")
                .select(selectColumns)
                .innerJoin("rag_documents")
                .as("d")
                .on("d.id = c.document_id");
    }

    private QueryColumn[] searchSelectColumns() {
        return new QueryColumn[] {
                raw("c.id AS chunk_id"),
                raw("c.document_id"),
                raw("d.title"),
                raw("d.source_type"),
                raw("d.source_uri"),
                raw("d.external_ref"),
                raw("c.index_generation"),
                raw("c.chunk_index"),
                raw("c.chunk_text"),
                raw("c.vector_id"),
                raw("c.metadata")
        };
    }

    private QueryColumn[] appendSearchColumn(QueryColumn column) {
        QueryColumn[] base = searchSelectColumns();
        QueryColumn[] columns = new QueryColumn[base.length + 1];
        System.arraycopy(base, 0, columns, 0, base.length);
        columns[base.length] = column;
        return columns;
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

    private RagChunkRecord toRecord(RagChunkEntity entity) {
        return new RagChunkRecord(
                entity.getId(),
                entity.getDocumentId(),
                entity.getIndexGeneration(),
                entity.getChunkIndex(),
                entity.getChunkText(),
                entity.getChunkHash(),
                entity.getCharCount(),
                entity.getTokenCount(),
                entity.getVectorId(),
                jsonCodec.readMap(entity.getMetadata()),
                entity.getCreatedAt(),
                entity.getUpdatedAt()
        );
    }

    private RagChunkSearchRecord toSearchRecord(Row row) {
        return new RagChunkSearchRecord(
                RagFlexRows.longValue(row, "chunk_id"),
                RagFlexRows.longValue(row, "document_id"),
                RagFlexRows.string(row, "title"),
                RagFlexRows.string(row, "source_type"),
                RagFlexRows.string(row, "source_uri"),
                RagFlexRows.string(row, "external_ref"),
                RagFlexRows.longValue(row, "index_generation"),
                RagFlexRows.intValue(row, "chunk_index"),
                RagFlexRows.string(row, "chunk_text"),
                RagFlexRows.string(row, "vector_id"),
                RagFlexRows.jsonMap(jsonCodec, row, "metadata")
        );
    }

    private RawQueryColumn raw(String expression, Object... args) {
        return new RawQueryColumn(expression, args);
    }

    private String placeholders(int count) {
        return String.join(",", java.util.Collections.nCopies(count, "?"));
    }

    private String likePattern(String token) {
        return "%" + token
                .replace("\\", "\\\\")
                .replace("%", "\\%")
                .replace("_", "\\_") + "%";
    }
}
