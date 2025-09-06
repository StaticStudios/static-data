package net.staticstudios.data.data.collection;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.PersistentCollection;
import net.staticstudios.data.UniqueData;
import net.staticstudios.data.data.DataHolder;
import net.staticstudios.data.key.CollectionKey;

import java.util.Arrays;

public abstract class SimplePersistentCollection<T> implements PersistentCollection<T> {
    private final DataHolder holder;
    private final String schema;
    private final String table;
    private final String entryIdColumn;
    private final String linkingColumn;
    private final String dataColumn;
    private final Class<T> dataType;

    protected SimplePersistentCollection(DataHolder holder, Class<T> dataType, String schema, String table, String entryIdColumn, String linkingColumn, String dataColumn) {
        this.holder = holder;
        this.schema = schema;
        this.table = table;
        this.entryIdColumn = entryIdColumn;
        this.linkingColumn = linkingColumn;
        this.dataColumn = dataColumn;
        this.dataType = dataType;
    }

    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
    }

    public String getEntryIdColumn() {
        return entryIdColumn;
    }

    public String getLinkingColumn() {
        return linkingColumn;
    }

    public String getDataColumn() {
        return dataColumn;
    }

    @Override
    public UniqueData getRootHolder() {
        return this.holder.getRootHolder();
    }

    @Override
    public DataManager getDataManager() {
        return this.holder.getDataManager();
    }

    @Override
    public CollectionKey getKey() {
        return new CollectionKey(
                schema,
                table,
                linkingColumn,
                dataColumn,
                getRootHolder().getId()
        );
    }

    @Override
    public Class<T> getDataType() {
        return dataType;
    }

    @Override
    public String toString() {
        return Arrays.toString(toArray());
    }
}
