package net.staticstudios.data.util;

public record ColumnMetadata(String name, Class<?> type, boolean nullable, boolean indexed, String table,
                             String schema) {
}
