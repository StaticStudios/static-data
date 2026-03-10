package net.staticstudios.data.util;

import java.util.List;
import java.util.Objects;

public class SelectQuery {
    private final String tag;
    private final String sql;
    private final List<Object> values;

    public SelectQuery(String tag, String sql, List<Object> values) {
        this.tag = tag;
        this.sql = sql;
        this.values = List.copyOf(values);
    }

    public String getSql() {
        return sql;
    }

    public List<Object> getValues() {
        return values;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof SelectQuery other)) return false;
        return Objects.equals(tag, other.tag) && Objects.equals(sql, other.sql) && Objects.equals(values, other.values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tag, sql, values);
    }

    @Override
    public String toString() {
        return "SelectQuery[" +
                "tag=" + tag + ", " +
                "sql=" + sql + ", " +
                "values=" + values + ']';
    }
}
