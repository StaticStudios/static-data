package net.staticstudios.data.key;

import net.staticstudios.data.data.value.persistent.PersistentValue;

import java.util.UUID;

public class CellKey extends DatabaseKey {
    private final String schema;
    private final String table;
    private final String column;
    private final String idColumn;

    public CellKey(String schema, String table, String column, UUID rootHolderId, String idColumn) {
        super(schema, table, column, rootHolderId, idColumn);
        this.schema = schema;
        this.table = table;
        this.column = column;
        this.idColumn = idColumn;
    }

    public CellKey(PersistentValue<?> data) {
        this(data.getSchema(), data.getTable(), data.getColumn(), data.getHolder().getRootHolder().getId(), data.getIdColumn());
    }

    @Override
    public String getSchema() {
        return schema;
    }

    @Override
    public String getTable() {
        return table;
    }

    public String getColumn() {
        return column;
    }

    public String getIdColumn() {
        return idColumn;
    }
}
