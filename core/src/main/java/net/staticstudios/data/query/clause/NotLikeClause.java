package net.staticstudios.data.query.clause;

import java.util.List;

public class NotLikeClause implements ValueClause {
    private final String schema;
    private final String table;
    private final String column;
    private final String format;

    public NotLikeClause(String schema, String table, String column, String format) {
        this.schema = schema;
        this.table = table;
        this.column = column;
        this.format = format;
    }

    @Override
    public List<Object> append(StringBuilder sb) {
        sb.append("\"").append(schema).append("\".\"").append(table).append("\".\"").append(column).append("\" NOT LIKE ?");
        return List.of(format);
    }
}
