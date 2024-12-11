package net.staticstudios.data.data.collection;

import net.staticstudios.data.DataKey;
import net.staticstudios.data.DataManager;
import net.staticstudios.data.PrimaryKey;
import net.staticstudios.data.data.DataHolder;
import net.staticstudios.data.data.Keyed;
import net.staticstudios.data.data.UniqueData;
import net.staticstudios.data.impl.PersistentCollectionValueManager;

public abstract class PersistentCollection<T> implements DataHolder, Keyed, java.util.Collection<T> {
    private final DataHolder holder;
    private final String schema;
    private final String table;
    private final String linkingColumn;
    private final String dataColumn;

    protected PersistentCollection(DataHolder holder, String schema, String table, String linkingColumn, String dataColumn) {
        this.holder = holder;
        this.schema = schema;
        this.table = table;
        this.linkingColumn = linkingColumn;
        this.dataColumn = dataColumn;
    }

    @SuppressWarnings("unchecked")
    public static <T> PersistentCollection<T> of(DataHolder holder, Class<T> data, String schema, String table, String linkingColumn, String dataColumn) {
        return new PersistentValueCollection<>(holder, schema, table, linkingColumn, dataColumn);
    }

    @SuppressWarnings("unchecked")
    public static <T extends UniqueData> PersistentCollection<T> of(DataHolder holder, Class<T> data, String schema, String table, String linkingColumn) {
        return (PersistentCollection<T>) new PersistentUniqueDataCollection<>(holder, schema, table, linkingColumn);
    }

    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
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
    public PrimaryKey getPKey() {
        return null; //todo: i think this is what we want? im not sure
    }

    @Override
    public DataKey getKey() {
        return PersistentCollectionValueManager.createCollectionKey(this);
    }
}
