package com.involutionhell.backend.rag.shared.persistence;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import org.apache.ibatis.type.BaseTypeHandler;
import org.apache.ibatis.type.JdbcType;

/**
 * Stores JSON text as jsonb on PostgreSQL and plain text on H2/test databases.
 */
public class JsonStringTypeHandler extends BaseTypeHandler<String> {

    @Override
    public void setNonNullParameter(PreparedStatement ps, int i, String parameter, JdbcType jdbcType)
            throws SQLException {
        if (isPostgreSql(ps)) {
            ps.setObject(i, parameter, Types.OTHER);
            return;
        }
        ps.setString(i, parameter);
    }

    @Override
    public String getNullableResult(ResultSet rs, String columnName) throws SQLException {
        return rs.getString(columnName);
    }

    @Override
    public String getNullableResult(ResultSet rs, int columnIndex) throws SQLException {
        return rs.getString(columnIndex);
    }

    @Override
    public String getNullableResult(CallableStatement cs, int columnIndex) throws SQLException {
        return cs.getString(columnIndex);
    }

    private boolean isPostgreSql(PreparedStatement statement) throws SQLException {
        return statement.getConnection()
                .getMetaData()
                .getDatabaseProductName()
                .toLowerCase()
                .contains("postgres");
    }
}
