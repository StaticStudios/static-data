package net.staticstudios.data.parse;

import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class SQLSchema {
    private final String name;
    private final Map<String, SQLTable> tables;

    public SQLSchema(String name) {
        this.name = name;
        this.tables = new HashMap<>();
    }

    public String getName() {
        return name;
    }

    public Set<SQLTable> getTables() {
        return new HashSet<>(tables.values());
    }

    public @Nullable SQLTable getTable(String tableName) {
        return tables.get(tableName);
    }

    public void addTable(SQLTable table) {
        if (table.getSchema() != this) {
            throw new IllegalArgumentException("Table does not belong to this schema");
        }
        if (tables.containsKey(table.getName())) {
            throw new IllegalArgumentException("Table with name " + table.getName() + " already exists in schema " + name);
        }

        tables.put(table.getName(), table);
    }
}
