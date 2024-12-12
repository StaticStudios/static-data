package net.staticstudios.data.key;

import net.staticstudios.data.data.collection.PersistentCollection;

import java.util.UUID;

public class CollectionEntryKey extends DatabaseKey {
    private final String schema;
    private final String table;
    private final String linkingColumn;
    private final String dataColumn;

    public CollectionEntryKey(String schema, String table, String linkingColumn, String dataColumn, UUID linkingId, UUID entryId) {
        super(schema, table, linkingColumn, dataColumn, linkingId, entryId);
        this.schema = schema;
        this.table = table;
        this.linkingColumn = linkingColumn;
        this.dataColumn = dataColumn;
    }

    public CollectionEntryKey(PersistentCollection<?> collection, UUID entryId) {
        this(collection.getSchema(), collection.getTable(), collection.getLinkingColumn(), collection.getDataColumn(), collection.getRootHolder().getId(), entryId);
    }

    @Override
    public String getSchema() {
        return schema;
    }

    @Override
    public String getTable() {
        return table;
    }
}
