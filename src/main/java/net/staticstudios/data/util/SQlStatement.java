package net.staticstudios.data.util;

import java.util.List;

public class SQlStatement {
    private final String sql;
    private final List<Object> values;

    public SQlStatement(String sql, List<Object> values) {
        this.sql = sql;
        this.values = values;
    }

    public String getSql() {
        return sql;
    }

    public List<Object> getValues() {
        return values;
    }
}
