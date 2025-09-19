package net.staticstudios.data.query;

public record InnerJoin(String schema, String table, String[] columns, String foreignSchema, String foreignTable,
                        String[] foreignColumns) {
}
