package net.staticstudios.data.util;

import org.jetbrains.annotations.NotNull;

import java.util.Objects;

public class ColumnMetadata {
    private final String schema;
    private final String table;
    private final String name;
    private final Class<?> type;
    private final boolean nullable;
    private final boolean indexed;
    private final @NotNull String encodedDefaultValue;

    public ColumnMetadata(String schema, String table, String name, Class<?> type, boolean nullable, boolean indexed,
                          @NotNull String encodedDefaultValue) {
        this.schema = schema;
        this.table = table;
        this.name = name;
        this.type = type;
        this.nullable = nullable;
        this.indexed = indexed;
        this.encodedDefaultValue = encodedDefaultValue;
    }

    public String schema() {
        return schema;
    }

    public String table() {
        return table;
    }

    public String name() {
        return name;
    }

    public Class<?> type() {
        return type;
    }

    public boolean nullable() {
        return nullable;
    }

    public boolean indexed() {
        return indexed;
    }

    public @NotNull String encodedDefaultValue() {
        return encodedDefaultValue;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (ColumnMetadata) obj;
        return Objects.equals(this.schema, that.schema) &&
                Objects.equals(this.table, that.table) &&
                Objects.equals(this.name, that.name) &&
                Objects.equals(this.type, that.type) &&
                this.nullable == that.nullable &&
                this.indexed == that.indexed &&
                Objects.equals(this.encodedDefaultValue, that.encodedDefaultValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schema, table, name, type, nullable, indexed, encodedDefaultValue);
    }

    @Override
    public String toString() {
        return "ColumnMetadata[" +
                "schema=" + schema + ", " +
                "table=" + table + ", " +
                "name=" + name + ", " +
                "type=" + type + ", " +
                "nullable=" + nullable + ", " +
                "indexed=" + indexed + ", " +
                "encodedDefaultValue=" + encodedDefaultValue + ']';
    }

}
