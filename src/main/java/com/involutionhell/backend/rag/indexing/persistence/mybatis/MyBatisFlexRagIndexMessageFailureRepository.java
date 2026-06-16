package com.involutionhell.backend.rag.indexing.persistence.mybatis;

import com.involutionhell.backend.rag.indexing.persistence.RagIndexMessageFailureRepository;
import com.involutionhell.backend.rag.shared.persistence.RagDatabaseDialect;
import com.involutionhell.backend.rag.shared.persistence.RagFlexJson;
import com.involutionhell.backend.rag.shared.support.RagJsonCodec;
import com.mybatisflex.core.query.QueryChain;
import com.mybatisflex.core.row.DbChain;
import java.util.Map;
import org.springframework.stereotype.Repository;

@Repository
public class MyBatisFlexRagIndexMessageFailureRepository implements RagIndexMessageFailureRepository {

    private final RagIndexMessageFailureMapper messageFailureMapper;
    private final RagJsonCodec jsonCodec;
    private final RagDatabaseDialect databaseDialect;

    public MyBatisFlexRagIndexMessageFailureRepository(
            RagIndexMessageFailureMapper messageFailureMapper,
            RagJsonCodec jsonCodec,
            RagDatabaseDialect databaseDialect
    ) {
        this.messageFailureMapper = messageFailureMapper;
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
        DbChain insert = DbChain.table("rag_index_message_failures")
                .set("message_id", messageId)
                .set("topic", topic)
                .set("delivery_attempt", deliveryAttempt)
                .set("failure_type", failureType)
                .set("error_message", errorMessage)
                .set("payload_base64", payloadBase64)
                .set("payload_preview", payloadPreview);
        RagFlexJson.setJson(
                        insert,
                        databaseDialect,
                        "properties_json",
                        jsonCodec.write(properties == null ? Map.of() : properties)
                )
                .save();
    }

    @Override
    public int countByMessageId(String messageId) {
        return Math.toIntExact(QueryChain.of(messageFailureMapper)
                .where("message_id = ?", messageId)
                .count());
    }
}
