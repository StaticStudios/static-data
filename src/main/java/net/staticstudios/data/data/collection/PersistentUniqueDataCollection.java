package net.staticstudios.data.data.collection;

import net.staticstudios.data.data.DataHolder;
import net.staticstudios.data.data.UniqueData;
import net.staticstudios.data.impl.DataTypeManager;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.function.Consumer;

public class PersistentUniqueDataCollection<T extends UniqueData> extends PersistentCollection<T> {
    private final PersistentValueCollection<UUID> holderIds;

    public PersistentUniqueDataCollection(DataHolder holder, Class<T> dataType, String schema, String table, String linkingColumn, String dataColumn) {
        super(holder, dataType, schema, table, linkingColumn, dataColumn);
        holderIds = new PersistentValueCollection<>(holder, UUID.class, schema, table, linkingColumn, dataColumn);
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
        if (o instanceof DataHolder) {
            return holderIds.contains(((DataHolder) o).getRootHolder().getId());
        }

        return false;
    }

    @Override
    public @NotNull Iterator<T> iterator() {
        return new Itr(holderIds.toArray(new UUID[0]));
    }

    @Override
    public @NotNull Object[] toArray() {
        Object[] objects = new Object[size()];
        int i = 0;
        for (UUID id : holderIds) {
            objects[i] = getDataManager().getUniqueData(id);
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
        System.out.println("Adding " + t);
        holderIds.getManager().addEntries(holderIds, Collections.singletonList(new CollectionEntry(t.getId(), t.getId())));
        return true;
    }

    @Override
    public boolean remove(Object o) {
        if (o instanceof DataHolder) {
            return holderIds.remove(((DataHolder) o).getRootHolder().getId());
        }

        return false;
    }

    @Override
    public boolean containsAll(@NotNull Collection<?> c) {
        List<UUID> ids = new ArrayList<>();
        for (Object o : c) {
            if (o instanceof DataHolder) {
                ids.add(((DataHolder) o).getRootHolder().getId());
            }
        }

        return holderIds.containsAll(ids);
    }

    @Override
    public boolean addAll(@NotNull Collection<? extends T> c) {
        List<CollectionEntry> entries = new ArrayList<>();
        for (T t : c) {
            entries.add(new CollectionEntry(t.getId(), getRootHolder().getId()));
        }

        holderIds.getManager().addEntries(holderIds, entries);
        return true;
    }

    @Override
    public boolean removeAll(@NotNull Collection<?> c) {
        List<UUID> ids = new ArrayList<>();
        for (Object o : c) {
            if (o instanceof DataHolder) {
                ids.add(((DataHolder) o).getRootHolder().getId());
            }
        }

        return holderIds.removeAll(ids);
    }

    @Override
    public boolean retainAll(@NotNull Collection<?> c) {
        List<UUID> ids = new ArrayList<>();
        for (Object o : c) {
            if (o instanceof DataHolder) {
                ids.add(((DataHolder) o).getRootHolder().getId());
            }
        }

        return holderIds.retainAll(ids);
    }

    @Override
    public void clear() {
        holderIds.clear();
    }

    @Override
    public Class<? extends DataTypeManager<?, ?>> getDataTypeManagerClass() {
        throw new UnsupportedOperationException("This collection does not have a data type manager");
    }

    @Override
    public String toString() {
        return "PersistentUniqueDataCollection{" +
                "holderIds=" + holderIds +
                '}';
    }

    private class Itr implements Iterator<T> {
        private final UUID[] ids;
        int cursor;       // index of next element to return
        int lastRet = -1; // index of last element returned; -1 if no such

        public Itr(UUID[] ids) {
            this.ids = ids;
        }

        public boolean hasNext() {
            return cursor != ids.length;
        }

        @SuppressWarnings("unchecked")
        public T next() {
            int i = cursor;
            if (!hasNext())
                throw new NoSuchElementException();
            cursor = i + 1;
            UUID id = ids[lastRet = i];
            return (T) getDataManager().getUniqueData(id);
        }

        public void remove() {
            if (lastRet < 0)
                throw new IllegalStateException();
            holderIds.remove(ids[lastRet]);

            lastRet = -1;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void forEachRemaining(Consumer<? super T> action) {
            Objects.requireNonNull(action);
            for (int i = cursor; i < ids.length; i++) {
                UUID id = ids[i];
                action.accept((T) getDataManager().getUniqueData(id));
            }
            cursor = ids.length;
        }
    }
}
