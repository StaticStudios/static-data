package net.staticstudios.data.data.collection;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.data.Data;
import net.staticstudios.data.data.DataHolder;
import net.staticstudios.data.data.UniqueData;
import net.staticstudios.data.key.CollectionKey;

import java.util.Arrays;

public abstract class PersistentCollection<T> implements DataHolder, java.util.Collection<T>, Data<T> {
    private final DataHolder holder;
    private final String schema;
    private final String table;
    private final String linkingColumn;
    private final String dataColumn;
    private final Class<T> dataType;

    protected PersistentCollection(DataHolder holder, Class<T> dataType, String schema, String table, String linkingColumn, String dataColumn) {
        this.holder = holder;
        this.schema = schema;
        this.table = table;
        this.linkingColumn = linkingColumn;
        this.dataColumn = dataColumn;
        this.dataType = dataType;
    }

    public static <T> PersistentCollection<T> of(DataHolder holder, Class<T> data, String schema, String table, String linkingColumn, String dataColumn) {
        //todo: lookup the class and see if its supported or has a serializer, otherwise throw an exception
        return new PersistentValueCollection<>(holder, data, schema, table, linkingColumn, dataColumn);
    }

//    @SuppressWarnings("unchecked")
//    public static <T extends UniqueData> PersistentCollection<T> of(DataHolder holder, Class<T> data, String schema, String table, String linkingColumn) {
//        return (PersistentCollection<T>) new PersistentUniqueDataCollection<>(holder, data, schema, table, linkingColumn, linkingColumn);
//    }

    public static <T extends UniqueData> PersistentCollection<T> oneToMany(DataHolder holder, Class<T> data, String schema, String table, String linkingColumn) {
        return new PersistentUniqueDataCollection<>(holder, data, schema, table, linkingColumn, "id");
    }

    public static <T extends UniqueData> PersistentCollection<T> oneToMany(DataHolder holder, Class<T> data, String schema, String table, String linkingColumn, String dataIdColumn) {
        return new PersistentUniqueDataCollection<>(holder, data, schema, table, linkingColumn, dataIdColumn);
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
    public CollectionKey getKey() {
        return new CollectionKey(this);
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
