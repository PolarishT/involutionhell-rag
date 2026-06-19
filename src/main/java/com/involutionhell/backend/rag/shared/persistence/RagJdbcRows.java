package com.involutionhell.backend.rag.shared.persistence;

import com.involutionhell.backend.rag.shared.support.RagJsonCodec;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.Map;

public final class RagJdbcRows {

    private RagJdbcRows() {
    }

    public static String string(ResultSet resultSet, String column) throws SQLException {
        Object value = resultSet.getObject(column);
        if (value == null) {
            return null;
        }
        if (value instanceof Clob clob) {
            return clob.getSubString(1, Math.toIntExact(clob.length()));
        }
        return String.valueOf(value);
    }

    public static Long longValue(ResultSet resultSet, String column) throws SQLException {
        Object value = resultSet.getObject(column);
        if (value == null) {
            return null;
        }
        return value instanceof Number number ? number.longValue() : Long.valueOf(String.valueOf(value));
    }

    public static Integer intValue(ResultSet resultSet, String column) throws SQLException {
        Object value = resultSet.getObject(column);
        if (value == null) {
            return null;
        }
        return value instanceof Number number ? number.intValue() : Integer.valueOf(String.valueOf(value));
    }

    public static Boolean booleanValue(ResultSet resultSet, String column) throws SQLException {
        Object value = resultSet.getObject(column);
        if (value == null) {
            return null;
        }
        return value instanceof Boolean bool ? bool : Boolean.valueOf(String.valueOf(value));
    }

    public static OffsetDateTime offsetDateTime(ResultSet resultSet, String column) throws SQLException {
        return offsetDateTime(resultSet.getObject(column));
    }

    public static OffsetDateTime offsetDateTime(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof OffsetDateTime offsetDateTime) {
            return offsetDateTime;
        }
        if (value instanceof Timestamp timestamp) {
            return timestamp.toInstant().atOffset(ZoneOffset.UTC);
        }
        if (value instanceof LocalDateTime localDateTime) {
            return localDateTime.atOffset(ZoneOffset.UTC);
        }
        if (value instanceof Date date) {
            return date.toInstant().atOffset(ZoneOffset.UTC);
        }
        String text = String.valueOf(value);
        try {
            return OffsetDateTime.parse(text);
        } catch (RuntimeException ignored) {
            return LocalDateTime.parse(text).atOffset(ZoneOffset.UTC);
        }
    }

    public static Map<String, Object> jsonMap(
            RagJsonCodec jsonCodec,
            ResultSet resultSet,
            String column
    ) throws SQLException {
        return jsonCodec.readMap(string(resultSet, column));
    }
}
