package net.staticstudios.data.query.clause;

import java.util.List;

public class EqualsIngoreCaseClause implements ValueClause {
    private final String schema;
    private final String table;
    private final String column;
    private final String value;

    public EqualsIngoreCaseClause(String schema, String table, String column, String value) {
        this.schema = schema;
        this.table = table;
        this.column = column;
        this.value = value;
    }

    @Override
    public List<Object> append(StringBuilder sb) {
        sb.append("UPPER(\"").append(schema).append("\".\"").append(table).append("\".\"").append(column).append("\") = UPPER(?)");
        return List.of(value);
    }
}
