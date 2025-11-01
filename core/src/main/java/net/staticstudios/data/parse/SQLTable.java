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
    private final Set<ForeignKey> foreignKeys;
    private final Set<SQLTrigger> triggers;

    public SQLTable(SQLSchema schema, String name, List<ColumnMetadata> idColumns) {
        this.schema = schema;
        this.name = name;
        this.idColumns = idColumns;
        this.columns = new HashMap<>();
        this.foreignKeys = new HashSet<>();
        this.triggers = new HashSet<>();
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

    public void addForeignKey(ForeignKey foreignKey) {
        Preconditions.checkNotNull(foreignKey, "Foreign key cannot be null");
        ForeignKey existingKey = foreignKeys.stream()
                .filter(fk -> fk.getReferencedSchema().equals(foreignKey.getReferencedSchema()) &&
                        fk.getReferencedTable().equals(foreignKey.getReferencedTable()) &&
                        fk.getReferringSchema().equals(foreignKey.getReferringSchema()) &&
                        fk.getReferringTable().equals(foreignKey.getReferringTable()) &&
                        fk.getName().equals(foreignKey.getName())
                )
                .findFirst()
                .orElse(null);
        if (existingKey != null && !Objects.equals(existingKey, foreignKey)) {
            throw new IllegalArgumentException("Foreign key to " + foreignKey.getReferencedSchema() + "." + foreignKey.getReferencedTable() + " already exists and is different from the one being added! Existing: " + existingKey + ", New: " + foreignKey);
        }
        foreignKeys.add(foreignKey);
    }

    public Set<ForeignKey> getForeignKeys() {
        return Collections.unmodifiableSet(foreignKeys);
    }

    public void addTrigger(SQLTrigger trigger) {
        Preconditions.checkNotNull(trigger, "Trigger cannot be null");
        triggers.add(trigger);
    }

    public Set<SQLTrigger> getTriggers() {
        return Collections.unmodifiableSet(triggers);
    }

    public List<ColumnMetadata> getIdColumns() {
        return idColumns;
    }

    public void addColumn(SQLColumn column) {
        if (column.getTable() != this) {
            throw new IllegalArgumentException("Column does not belong to this referringTable");
        }
        Preconditions.checkNotNull(column, "Column cannot be null");
        SQLColumn existingColumn = columns.get(column.getName());
        if (existingColumn != null && !Objects.equals(existingColumn, column)) {
            throw new IllegalArgumentException("Column with name " + column.getName() + " already exists in referringTable " + name + " in referringSchema " + schema.getName() + " and is different from the one being added");
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
