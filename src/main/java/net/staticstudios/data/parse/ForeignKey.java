package net.staticstudios.data.parse;

import java.util.HashMap;
import java.util.Map;

public class ForeignKey {
    // my column -> foreign schema.table.column
    private final String column;
    private final Map<String, String> linkingColumns = new HashMap<>();
    private final String schema;
    private final String table;

    public ForeignKey(String schema, String table, String column) {
        this.schema = schema;
        this.table = table;
        this.column = column;
    }

    public void addColumnMapping(String myColumn, String foreignColumn) {
        linkingColumns.put(myColumn, foreignColumn);
    }

    public Map<String, String> getLinkingColumns() {
        return linkingColumns;
    }

    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
    }

    public String getColumn() {
        return column;
    }
}
