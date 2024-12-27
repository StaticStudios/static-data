package net.staticstudios.data.data.collection;

import net.staticstudios.data.data.Data;
import net.staticstudios.data.data.DataHolder;
import net.staticstudios.data.data.UniqueData;
import org.jetbrains.annotations.Blocking;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Iterator;
import java.util.function.Predicate;

public interface PersistentCollection<T> extends Collection<T>, DataHolder, Data<T> {

    static <T> SimplePersistentCollection<T> of(DataHolder holder, Class<T> data, String schema, String table, String linkingColumn, String dataColumn) {
        return new PersistentValueCollection<>(holder, data, schema, table, "id", linkingColumn, dataColumn);
    }

    static <T> SimplePersistentCollection<T> of(DataHolder holder, Class<T> data, String schema, String table, String entryIdColumn, String linkingColumn, String dataColumn) {
        return new PersistentValueCollection<>(holder, data, schema, table, entryIdColumn, linkingColumn, dataColumn);
    }

    static <T extends UniqueData> SimplePersistentCollection<T> oneToMany(DataHolder holder, Class<T> data, String schema, String table, String linkingColumn) {
        return new PersistentUniqueDataCollection<>(holder, data, schema, table, linkingColumn, "id");
    }

    static <T extends UniqueData> PersistentCollection<T> manyToMany(DataHolder holder, Class<T> data, String schema, String junctionTable, String thisIdColumn, String dataIdColumn) {
        return new PersistentManyToManyCollection<>(holder, data, schema, junctionTable, thisIdColumn, dataIdColumn);
    }

    @Blocking
    boolean add(Connection connection, T t) throws SQLException;

    @Blocking
    boolean addAll(Connection connection, java.util.Collection<? extends T> c) throws SQLException;

    @Blocking
    boolean remove(Connection connection, T t) throws SQLException;

    @Blocking
    boolean removeAll(Connection connection, java.util.Collection<? extends T> c) throws SQLException;

    @Blocking
    void clear(Connection connection) throws SQLException;

    @Blocking
    Iterator<T> iterator(Connection connection);

    @Blocking
    default boolean removeIf(Connection connection, Predicate<? super T> filter) throws SQLException {
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
