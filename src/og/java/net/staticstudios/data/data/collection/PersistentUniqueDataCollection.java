package net.staticstudios.data.data.collection;

import net.staticstudios.data.PersistentCollection;
import net.staticstudios.data.UniqueData;
import net.staticstudios.data.data.DataHolder;
import net.staticstudios.data.impl.PersistentCollectionManager;
import net.staticstudios.data.util.BatchInsert;
import net.staticstudios.data.util.DeletionStrategy;
import net.staticstudios.utils.ThreadUtils;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.function.Consumer;

public class PersistentUniqueDataCollection<T extends UniqueData> extends SimplePersistentCollection<T> {
    private final PersistentValueCollection<UUID> holderIds;

    public PersistentUniqueDataCollection(DataHolder holder, Class<T> dataType, String schema, String table, String linkingColumn, String dataColumn) {
        super(holder,
                dataType,
                schema,
                table,
                //The entryIdColumn is the id column of the data type, so just create a dummy instance so we can get the id column name
                holder.getDataManager().getIdColumn(dataType),
                linkingColumn,
                dataColumn);
        holderIds = new PersistentValueCollection<>(holder, UUID.class, schema, table, getEntryIdColumn(), linkingColumn, dataColumn);
        holderIds.deletionStrategy(DeletionStrategy.NO_ACTION);
        this.deletionStrategy(DeletionStrategy.UNLINK);
    }

    public PersistentValueCollection<UUID> getHolderIds() {
        return holderIds;
    }

    @Override
    public void addBatch(BatchInsert batch, List<T> holders) {
        PersistentCollectionManager manager = getDataManager().getPersistentCollectionManager();
        List<CollectionEntry> entries = new ArrayList<>();
        for (T holder : holders) {
            entries.add(new CollectionEntry(holder.getId(), holder.getId()));
        }

        batch.early(() -> manager.addEntriesToCache(holderIds, entries));
        batch.intermediate(connection -> manager.addUniqueDataEntryToDatabase(connection, this, entries));
    }

    @Override
    public DataHolder getHolder() {
        return holderIds.getHolder();
    }

    @Override
    public int size() {
        return holderIds.size();
    }

    @Override
    public boolean isEmpty() {
        return holderIds.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        if (o instanceof DataHolder holder) {
            return holderIds.contains(holder.getRootHolder().getId());
        }

        return false;
    }

    @Override
    public @NotNull Iterator<T> iterator() {
        return new NonBlockingItr(holderIds.toArray(new UUID[0]));
    }

    @Override
    public @NotNull Object[] toArray() {
        Object[] objects = new Object[size()];
        int i = 0;
        for (UUID id : holderIds) {
            objects[i] = getDataManager().get(getDataType(), id);
            i++;
        }

        return objects;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T1> T1 @NotNull [] toArray(T1[] a) {
        int size = size();
        if (a.length < size) {
            return (T1[]) Arrays.copyOf(toArray(), size, a.getClass());
        }

        System.arraycopy(toArray(), 0, a, 0, size);
        if (a.length > size) {
            a[size()] = null;
        }

        return a;
    }

    @Override
    public boolean add(T t) {
        PersistentCollectionManager manager = holderIds.getManager();
        List<CollectionEntry> toAdd = Collections.singletonList(new CollectionEntry(t.getId(), t.getId()));
        manager.addEntriesToCache(holderIds, toAdd);
        getDataManager().submitAsyncTask(connection -> manager.addUniqueDataEntryToDatabase(connection, this, toAdd));

        return true;
    }

    @Override
    public boolean remove(Object o) {
        if (o instanceof DataHolder holder) {
            UUID id = holder.getRootHolder().getId();
            if (holderIds.contains(id)) {
                List<UUID> toRemove = Collections.singletonList(id);
                holderIds.getManager().removeFromUniqueDataCollectionInMemory(holderIds, toRemove);
                getDataManager().submitAsyncTask(connection -> holderIds.getManager().removeFromUniqueDataCollectionInDatabase(connection, holderIds, toRemove));

                return true;
            }
        }

        return false;
    }

    @Override
    public boolean containsAll(@NotNull Collection<?> c) {
        boolean containsAll = true;
        List<UUID> ids = new ArrayList<>();
        for (Object o : c) {
            if (o instanceof DataHolder) {
                ids.add(((DataHolder) o).getRootHolder().getId());
            } else {
                containsAll = false;
                break;
            }
        }

        return containsAll && holderIds.containsAll(ids);
    }

    @Override
    public boolean addAll(@NotNull Collection<? extends T> c) {
        List<CollectionEntry> toAdd = new ArrayList<>();
        for (T t : c) {
            toAdd.add(new CollectionEntry(t.getId(), t.getId()));
        }

        PersistentCollectionManager manager = holderIds.getManager();
        manager.addEntriesToCache(holderIds, toAdd);
        getDataManager().submitAsyncTask(connection -> manager.addUniqueDataEntryToDatabase(connection, this, toAdd));


        return true;
    }

    @Override
    public boolean removeAll(@NotNull Collection<?> c) {
        List<UUID> toRemove = new ArrayList<>();
        for (Object o : c) {
            if (o instanceof DataHolder) {
                if (holderIds.contains(((DataHolder) o).getRootHolder().getId())) {
                    toRemove.add(((DataHolder) o).getRootHolder().getId());
                }
            }
        }

        holderIds.getManager().removeFromUniqueDataCollectionInMemory(holderIds, toRemove);
        getDataManager().submitAsyncTask(connection -> holderIds.getManager().removeFromUniqueDataCollectionInDatabase(connection, holderIds, toRemove));

        return !toRemove.isEmpty();
    }

    @Override
    public boolean retainAll(@NotNull Collection<?> c) {
        List<UUID> ids = new ArrayList<>();
        for (Object o : c) {
            if (o instanceof DataHolder) {
                if (holderIds.contains(((DataHolder) o).getRootHolder().getId())) {
                    ids.add(((DataHolder) o).getRootHolder().getId());
                }
            }
        }

        List<UUID> toRemove = new ArrayList<>();
        for (UUID id : holderIds) {
            if (!ids.contains(id)) {
                toRemove.add(id);
            }
        }

        holderIds.getManager().removeFromUniqueDataCollectionInMemory(holderIds, toRemove);
        getDataManager().submitAsyncTask(connection -> holderIds.getManager().removeFromUniqueDataCollectionInDatabase(connection, holderIds, toRemove));

        return !toRemove.isEmpty();
    }

    @Override
    public void clear() {
        List<UUID> toRemove = new ArrayList<>(holderIds);
        holderIds.getManager().removeFromUniqueDataCollectionInMemory(holderIds, toRemove);
        getDataManager().submitAsyncTask(connection -> holderIds.getManager().removeFromUniqueDataCollectionInDatabase(connection, holderIds, toRemove));
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" +
                "dataType=" + getDataType() +
                ", holderIds=" + holderIds +
                '}';
    }

    @Override
    public boolean addNow(T t) {
        PersistentCollectionManager manager = holderIds.getManager();
        List<CollectionEntry> toAdd = Collections.singletonList(new CollectionEntry(t.getId(), t.getId()));
        manager.addEntriesToCache(holderIds, toAdd);
        getDataManager().submitBlockingTask(connection -> manager.addUniqueDataEntryToDatabase(connection, this, toAdd));
        return true;
    }

    @Override
    public boolean addAllNow(Collection<? extends T> c) {
        List<CollectionEntry> toAdd = new ArrayList<>();
        for (T t : c) {
            toAdd.add(new CollectionEntry(t.getId(), t.getId()));
        }

        PersistentCollectionManager manager = holderIds.getManager();
        manager.addEntriesToCache(holderIds, toAdd);
        getDataManager().submitBlockingTask(connection -> manager.addUniqueDataEntryToDatabase(connection, this, toAdd));
        return true;
    }

    @Override
    public boolean removeNow(T t) {
        UUID id = t.getId();
        if (holderIds.contains(id)) {
            List<UUID> toRemove = Collections.singletonList(id);
            holderIds.getManager().removeFromUniqueDataCollectionInMemory(holderIds, toRemove);
            getDataManager().submitBlockingTask(connection -> holderIds.getManager().removeFromUniqueDataCollectionInDatabase(connection, holderIds, toRemove));
            return true;
        }

        return false;
    }

    @Override
    public boolean removeAllNow(Collection<? extends T> c) {
        List<UUID> toRemove = new ArrayList<>();
        for (T t : c) {
            if (holderIds.contains(t.getId())) {
                toRemove.add(t.getId());
            }
        }

        holderIds.getManager().removeFromUniqueDataCollectionInMemory(holderIds, toRemove);
        getDataManager().submitBlockingTask(connection -> holderIds.getManager().removeFromUniqueDataCollectionInDatabase(connection, holderIds, toRemove));
        return !toRemove.isEmpty();
    }

    @Override
    public void clearNow() {
        List<UUID> toRemove = new ArrayList<>(holderIds);
        holderIds.getManager().removeFromUniqueDataCollectionInMemory(holderIds, toRemove);
        getDataManager().submitBlockingTask(connection -> holderIds.getManager().removeFromUniqueDataCollectionInDatabase(connection, holderIds, toRemove));
    }

    @Override
    public Iterator<T> blockingIterator() {
        return new BlockingItr(holderIds.toArray(new UUID[0]));
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
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (!super.equals(obj)) return false;
        PersistentUniqueDataCollection<?> that = (PersistentUniqueDataCollection<?>) obj;
        return holderIds.equals(that.holderIds);
    }

    @Override
    public int hashCode() {
        return holderIds.hashCode();
    }

    @Override
    public PersistentUniqueDataCollection<T> deletionStrategy(DeletionStrategy strategy) {
        holderIds.deletionStrategy(strategy);
        return this;
    }

    @Override
    public @NotNull DeletionStrategy getDeletionStrategy() {
        return holderIds.getDeletionStrategy();
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
            PersistentCollectionManager manager = holderIds.getManager();
            manager.removeFromUniqueDataCollectionInMemory(holderIds, Collections.singletonList(id));
            getDataManager().submitAsyncTask(connection -> manager.removeFromUniqueDataCollectionInDatabase(connection, holderIds, Collections.singletonList(id)));

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
            PersistentCollectionManager manager = holderIds.getManager();
            manager.removeFromUniqueDataCollectionInMemory(holderIds, Collections.singletonList(id));
            getDataManager().submitBlockingTask(connection -> manager.removeFromUniqueDataCollectionInDatabase(connection, holderIds, Collections.singletonList(id)));

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
