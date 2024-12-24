package net.staticstudios.data.data.collection;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.data.Data;
import net.staticstudios.data.data.DataHolder;
import net.staticstudios.data.data.UniqueData;
import net.staticstudios.data.key.CollectionKey;
import org.jetbrains.annotations.Blocking;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.function.Predicate;

public abstract class PersistentCollection<T> implements DataHolder, java.util.Collection<T>, Data<T> {
    private final DataHolder holder;
    private final String schema;
    private final String table;
    private final String entryIdColumn;
    private final String linkingColumn;
    private final String dataColumn;
    private final Class<T> dataType;

    protected PersistentCollection(DataHolder holder, Class<T> dataType, String schema, String table, String entryIdColumn, String linkingColumn, String dataColumn) {
        this.holder = holder;
        this.schema = schema;
        this.table = table;
        this.entryIdColumn = entryIdColumn;
        this.linkingColumn = linkingColumn;
        this.dataColumn = dataColumn;
        this.dataType = dataType;
    }

    public static <T> PersistentCollection<T> of(DataHolder holder, Class<T> data, String schema, String table, String linkingColumn, String dataColumn) {
        return new PersistentValueCollection<>(holder, data, schema, table, "id", linkingColumn, dataColumn);
    }

    public static <T> PersistentCollection<T> of(DataHolder holder, Class<T> data, String schema, String table, String entryIdColumn, String linkingColumn, String dataColumn) {
        return new PersistentValueCollection<>(holder, data, schema, table, entryIdColumn, linkingColumn, dataColumn);
    }

//    @SuppressWarnings("unchecked")
//    public static <T extends UniqueData> PersistentCollection<T> of(DataHolder holder, Class<T> data, String schema, String table, String linkingColumn) {
//        return (PersistentCollection<T>) new PersistentUniqueDataCollection<>(holder, data, schema, table, linkingColumn, linkingColumn);
//    }

    public static <T extends UniqueData> PersistentCollection<T> oneToMany(DataHolder holder, Class<T> data, String schema, String table, String linkingColumn) {
        return new PersistentUniqueDataCollection<>(holder, data, schema, table, linkingColumn, "id");
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

    @Blocking
    public abstract boolean add(Connection connection, T t) throws SQLException;

    @Blocking
    public abstract boolean addAll(Connection connection, java.util.Collection<? extends T> c) throws SQLException;

    @Blocking
    public abstract boolean remove(Connection connection, T t) throws SQLException;

    @Blocking
    public abstract boolean removeAll(Connection connection, java.util.Collection<? extends T> c) throws SQLException;

    @Blocking
    public abstract void clear(Connection connection) throws SQLException;

    @Blocking
    public abstract Iterator<T> iterator(Connection connection);

    @Blocking
    public boolean removeIf(Connection connection, Predicate<? super T> filter) throws SQLException {
        boolean removed = false;
        Iterator<T> each = iterator(connection);
        while (each.hasNext()) {
            if (filter.test(each.next())) {
                try {
                    each.remove();
                } catch (Exception e) {
                    if (e.getCause() instanceof SQLException) {
                        throw (SQLException) e.getCause();
                    }
                }
                removed = true;
            }
        }
        return removed;
    }
}
