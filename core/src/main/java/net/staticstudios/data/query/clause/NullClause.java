package net.staticstudios.data.query.clause;

import java.util.List;

public class NullClause implements ValueClause {
    private final String schema;
    private final String table;
    private final String column;

    public NullClause(String schema, String table, String column) {
        this.schema = schema;
        this.table = table;
        this.column = column;
    }

    @Override
    public List<Object> append(StringBuilder sb) {
        sb.append("\"").append(schema).append("\".\"").append(table).append("\".\"").append(column).append("\" IS NULL");
        return List.of();
    }
}
