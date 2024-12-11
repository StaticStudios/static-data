package net.staticstudios.data.impl;

import java.sql.Timestamp;
import java.util.Map;

public class PostgresNotification {
    private final Timestamp timestamp;
    private final String schema;
    private final String table;
    private final PostgresOperation operation;
    private final Map<String, Object> data;

    public PostgresNotification(Timestamp timestamp, String schema, String table, PostgresOperation operation, Map<String, Object> data) {
        this.timestamp = timestamp;
        this.schema = schema;
        this.table = table;
        this.operation = operation;
        this.data = data;
    }

}
