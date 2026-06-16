package com.involutionhell.backend.rag.shared.persistence;

import com.mybatisflex.core.row.DbChain;
import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.springframework.stereotype.Component;

@Component
public class RagDatabaseDialect {

    private final DataSource dataSource;
    private volatile Boolean postgreSql;
    private volatile Boolean pgTrgmEnabled;

    public RagDatabaseDialect(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public boolean isPostgreSql() {
        Boolean cached = postgreSql;
        if (cached != null) {
            return cached;
        }
        synchronized (this) {
            cached = postgreSql;
            if (cached == null) {
                cached = detectPostgreSql();
                postgreSql = cached;
            }
        }
        return cached;
    }

    public boolean isPgTrgmEnabled() {
        if (!isPostgreSql()) {
            pgTrgmEnabled = false;
            return false;
        }
        Boolean cached = pgTrgmEnabled;
        if (cached != null) {
            return cached;
        }
        synchronized (this) {
            cached = pgTrgmEnabled;
            if (cached == null) {
                cached = detectPgTrgmEnabled();
                pgTrgmEnabled = cached;
            }
        }
        return cached;
    }

    private boolean detectPostgreSql() {
        try (Connection connection = dataSource.getConnection()) {
            return connection.getMetaData()
                    .getDatabaseProductName()
                    .toLowerCase()
                    .contains("postgres");
        } catch (SQLException exception) {
            throw new IllegalStateException("无法识别数据库类型", exception);
        }
    }

    private boolean detectPgTrgmEnabled() {
        try {
            return DbChain.table("pg_extension")
                    .where("extname = ?", "pg_trgm")
                    .exists();
        } catch (RuntimeException exception) {
            return false;
        }
    }
}
