package net.staticstudios.data.query.clause;

import java.util.List;

public class NotBetweenClause implements ValueClause {
    private final String schema;
    private final String table;
    private final String column;
    private final Object min;
    private final Object max;

    public NotBetweenClause(String schema, String table, String column, Object min, Object max) {
        this.schema = schema;
        this.table = table;
        this.column = column;
        this.min = min;
        this.max = max;
    }

    @Override
    public List<Object> append(StringBuilder sb) {
        sb.append("\"").append(schema).append("\".\"").append(table).append("\".\"").append(column).append("\" NOT BETWEEN ? AND ?");
        return List.of(min, max);
    }
}
