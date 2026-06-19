package com.involutionhell.backend.rag.retrieval.persistence.jdbc;

import com.involutionhell.backend.rag.retrieval.persistence.RagUserRepository;
import com.involutionhell.backend.rag.shared.persistence.RagNestedTransactionExecutor;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
public class JdbcRagUserRepository implements RagUserRepository {

    private final JdbcClient jdbcClient;
    private final RagNestedTransactionExecutor nestedTransactionExecutor;

    public JdbcRagUserRepository(
            JdbcClient jdbcClient,
            RagNestedTransactionExecutor nestedTransactionExecutor
    ) {
        this.jdbcClient = jdbcClient;
        this.nestedTransactionExecutor = nestedTransactionExecutor;
    }

    @Override
    public void upsertSeen(String userId) {
        nestedTransactionExecutor.executeWithIntegrityRetry(() -> upsertSeenOnce(userId));
    }

    private void upsertSeenOnce(String userId) {
        int updated = jdbcClient.sql("""
                UPDATE rag_users
                SET last_seen_at = now()
                WHERE user_id = :userId
                """)
                .param("userId", userId)
                .update();
        if (updated > 0) {
            return;
        }
        jdbcClient.sql("INSERT INTO rag_users (user_id) VALUES (:userId)")
                .param("userId", userId)
                .update();
    }
}
