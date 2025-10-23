package net.staticstudios.data.query;

import java.util.Arrays;
import java.util.Objects;

public record InnerJoin(String schema, String table, String[] columns, String foreignSchema, String foreignTable,
                        String[] foreignColumns) {

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        InnerJoin that = (InnerJoin) obj;
        return Objects.equals(schema, that.schema) &&
                Objects.equals(table, that.table) &&
                Arrays.equals(columns, that.columns) &&
                Objects.equals(foreignSchema, that.foreignSchema) &&
                Objects.equals(foreignTable, that.foreignTable) &&
                Arrays.equals(foreignColumns, that.foreignColumns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schema, table, Arrays.hashCode(columns), foreignSchema, foreignTable, Arrays.hashCode(foreignColumns));
    }
}
