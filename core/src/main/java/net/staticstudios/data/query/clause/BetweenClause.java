package net.staticstudios.data.query.clause;

import java.util.List;

public class BetweenClause<N> implements ValueClause {
    private final String schema;
    private final String table;
    private final String column;
    private final N min;
    private final N max;

    public BetweenClause(String schema, String table, String column, N min, N max) {
        this.schema = schema;
        this.table = table;
        this.column = column;
        this.min = min;
        this.max = max;
    }

    @Override
    public List<Object> append(StringBuilder sb) {
        sb.append("\"").append(schema).append("\".\"").append(table).append("\".\"").append(column).append("\" BETWEEN ? AND ?");
        return List.of(min, max);
    }
}
