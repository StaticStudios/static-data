package net.staticstudios.data.parse;

import com.google.common.base.Preconditions;
import org.jetbrains.annotations.Nullable;

import java.util.*;

public class SQLTable {
    private final SQLSchema schema;
    private final String name;
    private final Map<String, SQLColumn> columns;

    public SQLTable(SQLSchema schema, String name) {
        this.schema = schema;
        this.name = name;
        this.columns = new HashMap<>();
    }

    public SQLSchema getSchema() {
        return schema;
    }

    public String getName() {
        return name;
    }

    public Set<SQLColumn> getColumns() {
        return new HashSet<>(columns.values());
    }

    public @Nullable SQLColumn getColumn(String columnName) {
        return columns.get(columnName);
    }

    public void addColumn(SQLColumn column) {
        if (column.getTable() != this) {
            throw new IllegalArgumentException("Column does not belong to this table");
        }
        Preconditions.checkNotNull(column, "Column cannot be null");
        SQLColumn existingColumn = columns.get(column.getName());
        if (existingColumn != null && !Objects.equals(existingColumn, column)) {
            throw new IllegalArgumentException("Column with name " + column.getName() + " already exists in table " + name + " in schema " + schema.getName() + " and is different from the one being added");
        }

        columns.put(column.getName(), column);
    }
}
