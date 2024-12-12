package net.staticstudios.data.key;

import net.staticstudios.data.data.collection.PersistentCollection;

import java.util.UUID;

public class CollectionKey extends DatabaseKey {
    private final String schema;
    private final String table;
    private final String linkingColumn;
    private final String dataColumn;

    public CollectionKey(String schema, String table, String linkingColumn, String dataColumn, UUID rootHolderId) {
        super(schema, table, linkingColumn, dataColumn, rootHolderId);
        this.schema = schema;
        this.table = table;
        this.linkingColumn = linkingColumn;
        this.dataColumn = dataColumn;
    }

    public CollectionKey(PersistentCollection<?> collection) {
        this(collection.getSchema(), collection.getTable(), collection.getLinkingColumn(), collection.getDataColumn(), collection.getRootHolder().getId());
    }

    @Override
    public String getSchema() {
        return schema;
    }

    @Override
    public String getTable() {
        return table;
    }

    public String getLinkingColumn() {
        return linkingColumn;
    }

    public String getDataColumn() {
        return dataColumn;
    }
}
