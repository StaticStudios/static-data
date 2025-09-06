package net.staticstudios.data.key;

import java.util.UUID;

/**
 * A collection key is unique to a one-to-many relationship instance.
 */
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
