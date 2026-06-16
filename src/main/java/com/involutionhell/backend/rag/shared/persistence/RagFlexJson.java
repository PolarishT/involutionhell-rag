package com.involutionhell.backend.rag.shared.persistence;

import com.mybatisflex.core.row.DbChain;
import com.mybatisflex.core.update.UpdateChain;

public final class RagFlexJson {

    private RagFlexJson() {
    }

    public static DbChain setJson(DbChain chain, RagDatabaseDialect dialect, String column, String json) {
        if (dialect.isPostgreSql()) {
            return chain.setRaw(column, jsonbLiteral(json));
        }
        return chain.set(column, json);
    }

    public static <T> UpdateChain<T> setJson(
            UpdateChain<T> chain,
            RagDatabaseDialect dialect,
            String column,
            String json
    ) {
        if (dialect.isPostgreSql()) {
            return chain.setRaw(column, jsonbLiteral(json), true);
        }
        return chain.set(column, json, true);
    }

    public static String jsonbLiteral(String json) {
        return json == null ? "NULL" : "'" + json.replace("'", "''") + "'::jsonb";
    }
}
