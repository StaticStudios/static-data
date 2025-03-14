package net.staticstudios.data.data.collection;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.PersistentCollection;
import net.staticstudios.data.UniqueData;
import net.staticstudios.data.data.DataHolder;
import net.staticstudios.data.impl.PersistentCollectionManager;
import net.staticstudios.data.key.CollectionKey;
import net.staticstudios.data.util.BatchInsert;
import net.staticstudios.data.util.DeletionStrategy;
import net.staticstudios.utils.ThreadUtils;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.function.Consumer;

public class PersistentManyToManyCollection<T extends UniqueData> implements PersistentCollection<T> {
    private final DataHolder holder;
    private final Class<T> dataType;
    private final String schema;
    private final String junctionTable;
    private final String thisIdColumn;
    private final String thatIdColumn;
    private DeletionStrategy deletionStrategy;

    public PersistentManyToManyCollection(DataHolder holder, Class<T> dataType, String schema, String junctionTable, String thisIdColumn, String thatIdColumn) {
        this.holder = holder;
        this.dataType = dataType;
        this.schema = schema;
        this.junctionTable = junctionTable;
        this.thisIdColumn = thisIdColumn;
        this.thatIdColumn = thatIdColumn;
        this.deletionStrategy = DeletionStrategy.UNLINK;
    }

    public String getSchema() {
        return schema;
    }

    public String getJunctionTable() {
        return junctionTable;
    }

    public String getThisIdColumn() {
        return thisIdColumn;
    }

    public String getThatIdColumn() {
        return thatIdColumn;
    }

    @Override
    public void addBatch(BatchInsert batch, List<T> holders) {
        PersistentCollectionManager manager = getManager();
        List<UUID> ids = holders.stream().map(UniqueData::getId).toList();

        batch.early(() -> manager.addToJunctionTableInMemory(this, ids));
        batch.intermediate(connection -> manager.addToJunctionTableInDatabase(connection, this, ids));
    }

    @Override
    public boolean addNow(T t) {
        PersistentCollectionManager manager = getManager();
        manager.addToJunctionTableInMemory(this, Collections.singletonList(t.getId()));
        getDataManager().submitBlockingTask(connection1 -> manager.addToJunctionTableInDatabase(connection1, this, Collections.singletonList(t.getId())));

        return true;
    }

    @Override
    public boolean addAllNow(Collection<? extends T> c) {
        List<UUID> ids = new ArrayList<>();
        for (T t : c) {
            ids.add(t.getId());
        }

        PersistentCollectionManager manager = getManager();
        manager.addToJunctionTableInMemory(this, ids);
        getDataManager().submitBlockingTask(connection -> manager.addToJunctionTableInDatabase(connection, this, ids));

        return true;
    }

    @Override
    public boolean removeNow(T t) {
        PersistentCollectionManager manager = getManager();
        manager.removeFromJunctionTableInMemory(this, Collections.singletonList(t.getId()));
        getDataManager().submitBlockingTask(connection -> manager.removeFromJunctionTableInDatabase(connection, this, Collections.singletonList(t.getId())));

        return true;
    }

    @Override
    public boolean removeAllNow(Collection<? extends T> c) {
        List<UUID> ids = new ArrayList<>();
        for (T t : c) {
            ids.add(t.getId());
        }

        PersistentCollectionManager manager = getManager();
        manager.removeFromJunctionTableInMemory(this, ids);
        getDataManager().submitBlockingTask(connection -> manager.removeFromJunctionTableInDatabase(connection, this, ids));

        return true;
    }

    @Override
    public void clearNow() {
        PersistentCollectionManager manager = getManager();
        List<UUID> ids = manager.getJunctionTableEntryIds(this);
        manager.removeFromJunctionTableInMemory(this, ids);
        getDataManager().submitBlockingTask(connection -> manager.removeFromJunctionTableInDatabase(connection, this, ids));
    }

    @Override
    public Iterator<T> blockingIterator() {
        return new BlockingItr(getManager().getJunctionTableEntryIds(this).toArray(UUID[]::new));
    }

    @Override
    public int size() {
        return getManager().getJunctionTableEntryIds(this).size();
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean contains(Object o) {
        if (!dataType.isInstance(o)) {
            return false;
        }

        return getManager().getJunctionTableEntryIds(this).contains(((UniqueData) o).getId());
    }

    @Override
    public @NotNull Iterator<T> iterator() {
        return new NonBlockingItr(getManager().getJunctionTableEntryIds(this).toArray(UUID[]::new));
    }

    @Override
    public @NotNull Object[] toArray() {
        return getManager().getJunctionTableEntryIds(this).stream().map(id -> getDataManager().get(dataType, id)).toArray();
    }

    @Override
    public @NotNull <T1> T1[] toArray(@NotNull T1[] a) {
        return getManager().getJunctionTableEntryIds(this).stream().map(id -> getDataManager().get(dataType, id)).toList().toArray(a);
    }

    @Override
    public boolean add(T t) {
        PersistentCollectionManager manager = getManager();
        manager.addToJunctionTableInMemory(this, Collections.singletonList(t.getId()));

        getDataManager().submitAsyncTask(connection -> manager.addToJunctionTableInDatabase(connection, this, Collections.singletonList(t.getId())));

        return true;
    }

    @Override
    public boolean remove(Object o) {
        if (!dataType.isInstance(o)) {
            return false;
        }

        PersistentCollectionManager manager = getManager();
        manager.removeFromJunctionTableInMemory(this, Collections.singletonList(((UniqueData) o).getId()));
        getDataManager().submitAsyncTask(connection -> manager.removeFromJunctionTableInDatabase(connection, this, Collections.singletonList(((UniqueData) o).getId())));

        return true;
    }

    @Override
    public boolean containsAll(@NotNull Collection<?> c) {
        for (Object o : c) {
            if (!dataType.isInstance(o)) {
                return false;
            }
        }

        return new HashSet<>(getManager().getJunctionTableEntryIds(this)).containsAll(c.stream().map(o -> ((UniqueData) o).getId()).toList());
    }

    @Override
    public boolean addAll(@NotNull Collection<? extends T> c) {
        List<UUID> ids = new ArrayList<>();
        for (T t : c) {
            ids.add(t.getId());
        }

        PersistentCollectionManager manager = getManager();
        manager.addToJunctionTableInMemory(this, ids);
        getDataManager().submitAsyncTask(connection -> manager.addToJunctionTableInDatabase(connection, this, ids));

        return true;
    }

    @Override
    public boolean removeAll(@NotNull Collection<?> c) {
        List<UUID> ids = new ArrayList<>();
        for (Object o : c) {
            if (dataType.isInstance(o)) {
                ids.add(((UniqueData) o).getId());
            }
        }

        PersistentCollectionManager manager = getManager();
        manager.removeFromJunctionTableInMemory(this, ids);
        getDataManager().submitAsyncTask(connection -> manager.removeFromJunctionTableInDatabase(connection, this, ids));

        return true;
    }

    @Override
    public boolean retainAll(@NotNull Collection<?> c) {
        List<UUID> ids = new ArrayList<>();
        for (Object o : c) {
            if (dataType.isInstance(o)) {
                ids.add(((UniqueData) o).getId());
            }
        }

        List<UUID> toRemove = new ArrayList<>();
        PersistentCollectionManager manager = getManager();
        for (UUID id : manager.getJunctionTableEntryIds(this)) {
            if (!ids.contains(id)) {
                toRemove.add(id);
            }
        }

        manager.removeFromJunctionTableInMemory(this, toRemove);
        getDataManager().submitAsyncTask(connection -> manager.removeFromJunctionTableInDatabase(connection, this, toRemove));

        return !toRemove.isEmpty();
    }

    @Override
    public void clear() {
        PersistentCollectionManager manager = getManager();
        List<UUID> ids = manager.getJunctionTableEntryIds(this);
        manager.removeFromJunctionTableInMemory(this, ids);
        getDataManager().submitAsyncTask(connection -> manager.removeFromJunctionTableInDatabase(connection, this, ids));
    }

    @Override
    public Class<T> getDataType() {
        return dataType;
    }

    @Override
    public CollectionKey getKey() {
        return new CollectionKey(schema, junctionTable, thisIdColumn, thatIdColumn, holder.getRootHolder().getId());
    }

    @Override
    public DataHolder getHolder() {
        return holder;
    }

    @Override
    public DataManager getDataManager() {
        return holder.getDataManager();
    }

    @Override
    public UniqueData getRootHolder() {
        return holder.getRootHolder();
    }

    private PersistentCollectionManager getManager() {
        return getDataManager().getPersistentCollectionManager();
    }

    @Override
    public PersistentCollection<T> onAdd(PersistentCollectionChangeHandler<T> handler) {
        getDataManager().getPersistentCollectionManager().addAddHandler(this, id -> {
            T t = getDataManager().get(getDataType(), (UUID) id);
            ThreadUtils.submit(() -> handler.onChange(t));
        });
        return this;
    }

    @Override
    public PersistentCollection<T> onRemove(PersistentCollectionChangeHandler<T> handler) {
        getDataManager().getPersistentCollectionManager().addRemoveHandler(this, id -> {
            T t = getDataManager().get(getDataType(), (UUID) id);
            ThreadUtils.submit(() -> handler.onChange(t));
        });
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PersistentManyToManyCollection<?> that = (PersistentManyToManyCollection<?>) o;
        return this.getKey().equals(that.getKey());
    }

    @Override
    public int hashCode() {
        return getKey().hashCode();
    }

    @Override
    public PersistentManyToManyCollection<T> deletionStrategy(DeletionStrategy strategy) {
        this.deletionStrategy = strategy;
        return this;
    }

    @Override
    public @NotNull DeletionStrategy getDeletionStrategy() {
        return deletionStrategy == null ? DeletionStrategy.NO_ACTION : deletionStrategy;
    }

    private class NonBlockingItr implements Iterator<T> {
        private final UUID[] ids;
        int cursor;       // index of next element to return
        int lastRet = -1; // index of last element returned; -1 if no such

        public NonBlockingItr(UUID[] ids) {
            this.ids = ids;
        }

        public boolean hasNext() {
            return cursor != ids.length;
        }

        public T next() {
            int i = cursor;
            if (!hasNext())
                throw new NoSuchElementException();
            cursor = i + 1;
            UUID id = ids[lastRet = i];
            return getDataManager().get(getDataType(), id);
        }

        public void remove() {
            if (lastRet < 0)
                throw new IllegalStateException();
            UUID id = ids[lastRet];
            PersistentCollectionManager manager = getManager();
            manager.removeFromJunctionTableInMemory(PersistentManyToManyCollection.this, Collections.singletonList(id));
            getDataManager().submitAsyncTask(connection -> manager.removeFromJunctionTableInDatabase(connection, PersistentManyToManyCollection.this, Collections.singletonList(id)));

            lastRet = -1;
        }

        @Override
        public void forEachRemaining(Consumer<? super T> action) {
            Objects.requireNonNull(action);
            for (int i = cursor; i < ids.length; i++) {
                UUID id = ids[i];
                action.accept(getDataManager().get(getDataType(), id));
            }
            cursor = ids.length;
        }
    }

    private class BlockingItr implements Iterator<T> {
        private final UUID[] ids;
        int cursor;       // index of next element to return
        int lastRet = -1; // index of last element returned; -1 if no such

        public BlockingItr(UUID[] ids) {
            this.ids = ids;
        }

        public boolean hasNext() {
            return cursor != ids.length;
        }

        public T next() {
            int i = cursor;
            if (!hasNext())
                throw new NoSuchElementException();
            cursor = i + 1;
            UUID id = ids[lastRet = i];
            return getDataManager().get(getDataType(), id);
        }

        public void remove() {
            if (lastRet < 0)
                throw new IllegalStateException();
            UUID id = ids[lastRet];
            PersistentCollectionManager manager = getManager();
            manager.removeFromJunctionTableInMemory(PersistentManyToManyCollection.this, Collections.singletonList(id));
            getDataManager().submitBlockingTask(connection -> manager.removeFromJunctionTableInDatabase(connection, PersistentManyToManyCollection.this, Collections.singletonList(id)));

            lastRet = -1;
        }

        @Override
        public void forEachRemaining(Consumer<? super T> action) {
            Objects.requireNonNull(action);
            for (int i = cursor; i < ids.length; i++) {
                UUID id = ids[i];
                action.accept(getDataManager().get(getDataType(), id));
            }
            cursor = ids.length;
        }
    }
}
