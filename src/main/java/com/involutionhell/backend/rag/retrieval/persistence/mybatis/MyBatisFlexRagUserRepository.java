package com.involutionhell.backend.rag.retrieval.persistence.mybatis;

import com.involutionhell.backend.rag.retrieval.persistence.RagUserRepository;
import com.involutionhell.backend.rag.shared.persistence.RagNestedTransactionExecutor;
import com.mybatisflex.core.row.DbChain;
import com.mybatisflex.core.update.UpdateChain;
import org.springframework.stereotype.Repository;

@Repository
public class MyBatisFlexRagUserRepository implements RagUserRepository {

    private final RagUserMapper userMapper;
    private final RagNestedTransactionExecutor nestedTransactionExecutor;

    public MyBatisFlexRagUserRepository(
            RagUserMapper userMapper,
            RagNestedTransactionExecutor nestedTransactionExecutor
    ) {
        this.userMapper = userMapper;
        this.nestedTransactionExecutor = nestedTransactionExecutor;
    }

    @Override
    public void upsertSeen(String userId) {
        nestedTransactionExecutor.executeWithIntegrityRetry(() -> upsertSeenOnce(userId));
    }

    private void upsertSeenOnce(String userId) {
        boolean updated = UpdateChain.of(userMapper)
                .setRaw("last_seen_at", "now()", true)
                .where("user_id = ?", userId)
                .update();
        if (updated) {
            return;
        }
        DbChain.table("rag_users")
                .set("user_id", userId)
                .save();
    }
}
