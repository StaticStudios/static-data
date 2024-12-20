package net.staticstudios.data.impl;

import java.time.Instant;
import java.util.Map;

public class PostgresNotification {
    private final Instant instant;
    private final String schema;
    private final String table;
    private final PostgresOperation operation;
    private final Map<String, String> data;

    public PostgresNotification(Instant instant, String schema, String table, PostgresOperation operation, Map<String, String> dataValueMap) {
        this.instant = instant;
        this.schema = schema;
        this.table = table;
        this.operation = operation;
        this.data = dataValueMap;
    }

    public Instant getInstant() {
        return instant;
    }

    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
    }

    public PostgresOperation getOperation() {
        return operation;
    }

    public Map<String, String> getDataValueMap() {
        return data;
    }

    @Override
    public String toString() {
        return "PostgresNotification{" +
                "instant=" + instant +
                ", schema='" + schema + '\'' +
                ", table='" + table + '\'' +
                ", operation=" + operation +
                ", data=" + data +
                '}';
    }
}
