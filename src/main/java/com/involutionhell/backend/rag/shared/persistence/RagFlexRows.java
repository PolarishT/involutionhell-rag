package com.involutionhell.backend.rag.shared.persistence;

import com.involutionhell.backend.rag.shared.support.RagJsonCodec;
import com.mybatisflex.core.row.Row;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.Map;

public final class RagFlexRows {

    private RagFlexRows() {
    }

    public static String string(Row row, String column) {
        Object value = value(row, column);
        if (value == null) {
            return null;
        }
        if (value instanceof Clob clob) {
            return clobToString(clob);
        }
        return String.valueOf(value);
    }

    public static Long longValue(Row row, String column) {
        Object value = value(row, column);
        if (value == null) {
            return null;
        }
        if (value instanceof Number number) {
            return number.longValue();
        }
        return Long.valueOf(String.valueOf(value));
    }

    public static Integer intValue(Row row, String column) {
        Object value = value(row, column);
        if (value == null) {
            return null;
        }
        if (value instanceof Number number) {
            return number.intValue();
        }
        return Integer.valueOf(String.valueOf(value));
    }

    public static Boolean booleanValue(Row row, String column) {
        Object value = value(row, column);
        if (value == null) {
            return null;
        }
        if (value instanceof Boolean bool) {
            return bool;
        }
        return Boolean.valueOf(String.valueOf(value));
    }

    public static OffsetDateTime offsetDateTime(Row row, String column) {
        return offsetDateTime(value(row, column));
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

    public static Map<String, Object> jsonMap(RagJsonCodec jsonCodec, Row row, String column) {
        return jsonCodec.readMap(string(row, column));
    }

    public static Object value(Row row, String column) {
        return row == null ? null : row.getIgnoreCase(column);
    }

    private static String clobToString(Clob clob) {
        try {
            return clob.getSubString(1, Math.toIntExact(clob.length()));
        } catch (SQLException exception) {
            throw new IllegalStateException("读取 CLOB 字段失败", exception);
        }
    }
}
