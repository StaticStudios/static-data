package net.staticstudios.data.query.clause;

import java.util.List;

public class NotEqualsClause implements ValueClause {
    private final String schema;
    private final String table;
    private final String column;
    private final Object value;

    public NotEqualsClause(String schema, String table, String column, Object value) {
        this.schema = schema;
        this.table = table;
        this.column = column;
        this.value = value;
    }

    @Override
    public List<Object> append(StringBuilder sb) {
        sb.append("\"").append(schema).append("\".\"").append(table).append("\".\"").append(column).append("\" <> ?");
        return List.of(value);
    }
}
