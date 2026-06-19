package com.involutionhell.backend.rag.indexing.persistence.jdbc;

import com.involutionhell.backend.rag.indexing.persistence.RagIndexMessageFailureRepository;
import com.involutionhell.backend.rag.shared.persistence.RagDatabaseDialect;
import com.involutionhell.backend.rag.shared.persistence.RagJdbcJson;
import com.involutionhell.backend.rag.shared.support.RagJsonCodec;
import java.util.Map;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
public class JdbcRagIndexMessageFailureRepository implements RagIndexMessageFailureRepository {

    private final JdbcClient jdbcClient;
    private final RagJsonCodec jsonCodec;
    private final RagDatabaseDialect databaseDialect;

    public JdbcRagIndexMessageFailureRepository(
            JdbcClient jdbcClient,
            RagJsonCodec jsonCodec,
            RagDatabaseDialect databaseDialect
    ) {
        this.jdbcClient = jdbcClient;
        this.jsonCodec = jsonCodec;
        this.databaseDialect = databaseDialect;
    }

    @Override
    public void save(
            String messageId,
            String topic,
            int deliveryAttempt,
            String failureType,
            String errorMessage,
            String payloadBase64,
            String payloadPreview,
            Map<String, Object> properties
    ) {
        String propertiesParameter = RagJdbcJson.parameter(databaseDialect, "propertiesJson");
        jdbcClient.sql("""
                INSERT INTO rag_index_message_failures (
                    message_id, topic, delivery_attempt, failure_type, error_message,
                    payload_base64, payload_preview, properties_json
                ) VALUES (
                    :messageId, :topic, :deliveryAttempt, :failureType, :errorMessage,
                    :payloadBase64, :payloadPreview, %s
                )
                """.formatted(propertiesParameter))
                .param("messageId", messageId)
                .param("topic", topic)
                .param("deliveryAttempt", deliveryAttempt)
                .param("failureType", failureType)
                .param("errorMessage", errorMessage)
                .param("payloadBase64", payloadBase64)
                .param("payloadPreview", payloadPreview)
                .param("propertiesJson", jsonCodec.write(properties == null ? Map.of() : properties))
                .update();
    }

    @Override
    public int countByMessageId(String messageId) {
        Long count = jdbcClient.sql("""
                SELECT COUNT(*)
                FROM rag_index_message_failures
                WHERE message_id = :messageId
                """)
                .param("messageId", messageId)
                .query(Long.class)
                .single();
        return Math.toIntExact(count);
    }
}
