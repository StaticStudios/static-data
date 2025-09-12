package net.staticstudios.data;

import net.staticstudios.data.data.Data;
import net.staticstudios.data.data.DataHolder;
import net.staticstudios.data.data.collection.*;
import net.staticstudios.data.key.CollectionKey;
import net.staticstudios.data.util.BatchInsert;
import net.staticstudios.data.util.DeletionStrategy;
import org.jetbrains.annotations.Blocking;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;

/**
 * Represents a collection of data that is stored in a database.
 * Depending on the implementation, this collection may represent a one-to-many or many-to-many relationship.
 *
 * @param <T> The type of data that this collection stores.
 */
public interface PersistentCollection<T> extends Collection<T>, DataHolder, Data<T> {

    /**
     * Create a new persistent collection with a one-to-many relationship.
     *
     * @param holder        The holder of this collection.
     * @param dataType      The type of data that this collection stores (must not be a subclass of {@link UniqueData}), see {@link #oneToMany} for one-to-many relationships with {@link UniqueData}.
     * @param schema        The schema of the table that this collection stores data in.
     * @param table         The table that this collection stores data in.
     * @param linkingColumn The column in the collection's table that links the data to the holder.
     * @param dataColumn    The column in the collection's table that stores the data itself.
     * @param <T>           The type of data that this collection stores (must not be a subclass of {@link UniqueData}), see {@link #oneToMany} for one-to-many relationships with {@link UniqueData}.
     * @return The persistent collection.
     */
    static <T> SimplePersistentCollection<T> of(DataHolder holder, Class<T> dataType, String schema, String table, String linkingColumn, String dataColumn) {
        return new PersistentValueCollection<>(holder, dataType, schema, table, "id", linkingColumn, dataColumn);
    }

    /**
     * Create a new persistent collection with a one-to-many relationship.
     *
     * @param holder        The holder of this collection.
     * @param dataType      The type of data that this collection stores (must be a subclass of {@link UniqueData}), see {@link #of} for one-to-many relationships with non-unique data.
     * @param schema        The schema of the table that this collection stores data in.
     * @param table         The table that this collection stores data in.
     * @param entryIdColumn The column in the collection's table that holds the id of each entry.
     * @param linkingColumn The column in the collection's table that links the data to the holder.
     * @param dataColumn    The column in the collection's table that stores the data itself.
     * @param <T>           The type of data that this collection stores (must be a subclass of {@link UniqueData}), see {@link #of} for one-to-many relationships with non-unique data.
     * @return The persistent collection.
     */
    static <T> SimplePersistentCollection<T> of(DataHolder holder, Class<T> dataType, String schema, String table, String entryIdColumn, String linkingColumn, String dataColumn) {
        return new PersistentValueCollection<>(holder, dataType, schema, table, entryIdColumn, linkingColumn, dataColumn);
    }

    /**
     * Create a new persistent collection with a one-to-many relationship.
     *
     * @param holder        The holder of this collection.
     * @param dataType      The type of data that this collection stores (must be a subclass of {@link UniqueData}), see {@link #of} for one-to-many relationships with non-unique data.
     * @param schema        The schema of the table that this collection stores data in.
     * @param table         The table that this collection stores data in.
     * @param linkingColumn The column in the collection's table that links the data to the holder.
     * @param <T>           The type of data that this collection stores (must be a subclass of {@link UniqueData}), see {@link #of} for one-to-many relationships with non-unique data.
     * @return The persistent collection.
     */
    static <T extends UniqueData> SimplePersistentCollection<T> oneToMany(DataHolder holder, Class<T> dataType, String schema, String table, String linkingColumn) {
        return new PersistentUniqueDataCollection<>(holder, dataType, schema, table, linkingColumn, holder.getDataManager().getIdColumn(dataType));
    }

    /**
     * Create a new persistent collection with a one-to-many relationship.
     *
     * @param holder        The holder of this collection.
     * @param dataType      The type of data that this collection stores (must be a subclass of {@link UniqueData})
     * @param schema        The schema of the junction table that links the data to the holder.
     * @param junctionTable The junction table that links the data to the holder.
     * @param thisIdColumn  The column in the junction table that links the holder to the data.
     * @param dataIdColumn  The column in the junction table that links the data to the holder.
     * @param <T>           The type of data that this collection stores (must be a subclass of {@link UniqueData})
     * @return The persistent collection.
     */
    static <T extends UniqueData> PersistentCollection<T> manyToMany(DataHolder holder, Class<T> dataType, String schema, String junctionTable, String thisIdColumn, String dataIdColumn) {
        return new PersistentManyToManyCollection<>(holder, dataType, schema, junctionTable, thisIdColumn, dataIdColumn);
    }

    @Blocking
    default void addBatch(BatchInsert batch, List<T> toAdd) {
        throw new UnsupportedOperationException("Batch insert is not supported for this collection type.");
    }

    /**
     * Similar to {@link #add(Object)}, but this method is blocking and will wait for the database update to complete.
     *
     * @param t The element to add.
     * @return true if the element was added, false otherwise.
     */
    @Blocking
    boolean addNow(T t);

    /**
     * Similar to {@link #addAll(Collection)}, but this method is blocking and will wait for the database update to complete.
     *
     * @param c The elements to add.
     * @return true if the elements were added, false otherwise.
     */
    @Blocking
    boolean addAllNow(java.util.Collection<? extends T> c);

    /**
     * Similar to {@link #remove(Object)}, but this method is blocking and will wait for the database update to complete.
     *
     * @param t The element to remove.
     * @return true if the element was removed, false otherwise.
     */
    @Blocking
    boolean removeNow(T t);

    /**
     * Similar to {@link #removeAll(Collection)}, but this method is blocking and will wait for the database update to complete.
     *
     * @param c The elements to remove.
     * @return true if the elements were removed, false otherwise.
     */
    @Blocking
    boolean removeAllNow(java.util.Collection<? extends T> c);

    /**
     * Similar to {@link #clear()}, but this method is blocking and will wait for the database update to complete.
     */
    @Blocking
    void clearNow();

    /**
     * Create a blocking iterator for this collection.
     * When calling {@link Iterator#remove()}, a blocking remove operation will be performed.
     * The remove operation will wait for the database update to complete.
     *
     * @return The blocking iterator.
     */
    @Blocking
    Iterator<T> blockingIterator();

    /**
     * Register a handler to be called when an element is added to this collection.
     * Note that handlers are called asynchronously.
     *
     * @param handler The handler to call.
     * @return This collection.
     */
    PersistentCollection<T> onAdd(PersistentCollectionChangeHandler<T> handler);

    /**
     * Register a handler to be called when an element is removed from this collection.
     * Note that handlers are called asynchronously.
     *
     * @param handler The handler to call.
     * @return This collection.
     */
    PersistentCollection<T> onRemove(PersistentCollectionChangeHandler<T> handler);

    CollectionKey getKey();

    PersistentCollection<T> deletionStrategy(DeletionStrategy strategy);

    /**
     * A blocking version of {@link #removeIf(Predicate)}.
     * This method will wait for the database update to complete.
     *
     * @param filter The filter to apply.
     * @return true if any elements were removed, false otherwise.
     */
    @Blocking
    default boolean removeIfNow(Predicate<? super T> filter) throws SQLException {
        boolean removed = false;
        Iterator<T> each = blockingIterator();
        while (each.hasNext()) {
            if (filter.test(each.next())) {
                each.remove();
                removed = true;
            }
        }
        return removed;
    }
}