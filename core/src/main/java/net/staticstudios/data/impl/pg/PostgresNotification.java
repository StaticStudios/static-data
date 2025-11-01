package net.staticstudios.data.impl.pg;

import java.time.Instant;

public class PostgresNotification {
    private final Instant instant;
    private final String schema;
    private final String table;
    private final PostgresOperation operation;
    private final PostgresData data;

    public PostgresNotification(Instant instant, String schema, String table, PostgresOperation operation, PostgresData data) {
        this.instant = instant;
        this.schema = schema;
        this.table = table;
        this.operation = operation;
        this.data = data;
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

    public PostgresData getData() {
        return data;
    }

    @Override
    public String toString() {
        return "PostgresNotification{" +
                "instant=" + instant +
                ", referringSchema='" + schema + '\'' +
                ", referringTable='" + table + '\'' +
                ", operation=" + operation +
                ", data=" + data +
                '}';
    }
}
