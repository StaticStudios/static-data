package net.staticstudios.data.parse;

import org.jetbrains.annotations.Nullable;

import java.util.Objects;

public class SQLColumn {
    private final Class<?> type;
    private final String name;
    private final boolean nullable;
    private final boolean indexed;
    private final boolean unique;
    private final @Nullable String defaultValue;
    private SQLTable table;

    public SQLColumn(SQLTable table, Class<?> type, String name, boolean nullable, boolean indexed, boolean unique, @Nullable String defaultValue) {
        this.table = table;
        this.type = type;
        this.name = name;
        this.nullable = nullable;
        this.indexed = indexed;
        this.unique = unique;
        this.defaultValue = defaultValue;
    }

    public void setTable(SQLTable table) {
        this.table = table;
    }

    public SQLTable getTable() {
        return table;
    }

    public Class<?> getType() {
        return type;
    }

    public String getName() {
        return name;
    }

    public boolean isNullable() {
        return nullable;
    }

    public boolean isIndexed() {
        return indexed;
    }

    public boolean isUnique() {
        return unique;
    }

    public @Nullable String getDefaultValue() {
        return defaultValue;
    }

    @Override
    public int hashCode() {
        return Objects.hash(table, type, name, nullable, indexed, unique, defaultValue);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        SQLColumn other = (SQLColumn) obj;
        return nullable == other.nullable &&
                indexed == other.indexed &&
                unique == other.unique &&
                Objects.equals(defaultValue, other.defaultValue) &&
                Objects.equals(type, other.type) &&
                Objects.equals(name, other.name);
    }

    @Override
    public String toString() {
        return "SQLColumn{" +
                ", type=" + type +
                ", name='" + name + '\'' +
                ", nullable=" + nullable +
                ", indexed=" + indexed +
                ", unique=" + unique +
                ", defaultValue='" + defaultValue + '\'' +
                '}';
    }
}
