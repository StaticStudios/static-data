package net.staticstudios.data.util;

public record SimpleColumnMetadata(String schema, String table, String name, Class<?> type) {

    public SimpleColumnMetadata(ColumnMetadata metadata) {
        this(metadata.schema(), metadata.table(), metadata.name(), metadata.type());
    }
}
