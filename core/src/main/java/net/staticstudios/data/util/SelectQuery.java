package net.staticstudios.data.util;

import java.util.List;
import java.util.Objects;

public class SelectQuery {
    private final String sql;
    private final List<Object> values;

    public SelectQuery(String sql, List<Object> values) {
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
        return Objects.equals(sql, other.sql) && Objects.equals(values, other.values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sql, values);
    }

    @Override
    public String toString() {
        return "SelectQuery[" +
                "sql=" + sql + ", " +
                "values=" + values + ']';
    }
}
