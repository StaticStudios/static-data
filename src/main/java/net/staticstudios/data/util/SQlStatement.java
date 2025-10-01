package net.staticstudios.data.util;

import java.util.List;

public class SQlStatement {
    private final String h2Sql;
    private final String pgSql;
    private final List<Object> values;

    public SQlStatement(String h2Sql, String pgSql, List<Object> values) {
        this.h2Sql = h2Sql;
        this.pgSql = pgSql;
        this.values = values;
    }

    public String getH2Sql() {
        return h2Sql;
    }

    public String getPgSql() {
        return pgSql;
    }

    public List<Object> getValues() {
        return values;
    }
}
