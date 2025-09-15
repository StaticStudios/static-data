package net.staticstudios.data.parse;

import com.google.common.base.Preconditions;
import net.staticstudios.data.util.ColumnMetadata;
import org.jetbrains.annotations.Nullable;

import java.util.*;

public class SQLTable {
    private final SQLSchema schema;
    private final String name;
    private final List<ColumnMetadata> idColumns;
    private final Map<String, SQLColumn> columns;
    private final List<ForeignKey> foreignKeys;

    public SQLTable(SQLSchema schema, String name, List<ColumnMetadata> idColumns) {
        this.schema = schema;
        this.name = name;
        this.idColumns = idColumns;
        this.columns = new HashMap<>();
        this.foreignKeys = new ArrayList<>();
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

    public List<ForeignKey> getForeignKeys() {
        return foreignKeys;
    }

    public List<ColumnMetadata> getIdColumns() {
        return idColumns;
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

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof SQLTable other)) return false;
        return Objects.equals(schema.getName(), other.schema.getName()) && Objects.equals(name, other.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schema.getName(), name);
    }
}
