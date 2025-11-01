package net.staticstudios.data.query.clause;

import java.util.List;

public class NotInClause implements ValueClause {
    private final String schema;
    private final String table;
    private final String column;
    private final Object[] values;

    public NotInClause(String schema, String table, String column, Object[] values) {
        this.schema = schema;
        this.table = table;
        this.column = column;
        this.values = values;
    }

    @Override
    public List<Object> append(StringBuilder sb) {
        sb.append("\"").append(schema).append("\".\"").append(table).append("\".\"").append(column).append("\" NOT IN (");
        for (int i = 0; i < values.length; i++) {
            sb.append("?");
            if (i < values.length - 1) {
                sb.append(", ");
            }
        }
        sb.append(")");
        return List.of(values);
    }
}
