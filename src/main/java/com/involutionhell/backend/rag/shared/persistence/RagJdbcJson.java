package com.involutionhell.backend.rag.shared.persistence;

public final class RagJdbcJson {

    private RagJdbcJson() {
    }

    public static String parameter(RagDatabaseDialect dialect, String parameterName) {
        String placeholder = ":" + parameterName;
        return dialect.isPostgreSql() ? "CAST(" + placeholder + " AS jsonb)" : placeholder;
    }
}
