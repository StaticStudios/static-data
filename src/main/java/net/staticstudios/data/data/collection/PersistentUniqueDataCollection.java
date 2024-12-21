package net.staticstudios.data.data.collection;

import net.staticstudios.data.data.DataHolder;
import net.staticstudios.data.data.UniqueData;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.function.Consumer;

public class PersistentUniqueDataCollection<T extends UniqueData> extends PersistentCollection<T> {
    private final PersistentValueCollection<UUID> holderIds;

    public PersistentUniqueDataCollection(DataHolder holder, Class<T> dataType, String schema, String table, String linkingColumn, String dataColumn) {
        super(holder,
                dataType,
                schema,
                table,
                //The entryIdColumn is the id column of the data type, so just create a dummy instance so we can get the id column name
                holder.getDataManager().createInstance(dataType, null).getIdentifier().getColumn(),
                linkingColumn,
                dataColumn);
        holderIds = new PersistentValueCollection<>(holder, UUID.class, schema, table, getEntryIdColumn(), linkingColumn, dataColumn);
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
        return new Itr(holderIds.toArray(new UUID[0]));
    }

    @Override
    public @NotNull Object[] toArray() {
        Object[] objects = new Object[size()];
        int i = 0;
        for (UUID id : holderIds) {
            objects[i] = getDataManager().getUniqueData(getDataType(), id);
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
        holderIds.getManager().addEntries(holderIds, Collections.singletonList(new CollectionEntry(t.getId(), t.getId())));
        return true;
    }

    @Override
    public boolean remove(Object o) {
        if (o instanceof DataHolder holder) {
            UUID id = holder.getRootHolder().getId();
            if (holderIds.contains(id)) {
                holderIds.getManager().removeFromUniqueDataCollection(holderIds, Collections.singletonList(id));
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
        List<CollectionEntry> entries = new ArrayList<>();
        for (T t : c) {
            entries.add(new CollectionEntry(t.getId(), t.getId()));
        }

        holderIds.getManager().addEntries(holderIds, entries);
        return true;
    }

    @Override
    public boolean removeAll(@NotNull Collection<?> c) {
        List<UUID> ids = new ArrayList<>();
        for (Object o : c) {
            if (o instanceof DataHolder) {
                if (holderIds.contains(((DataHolder) o).getRootHolder().getId())) {
                    ids.add(((DataHolder) o).getRootHolder().getId());
                }
            }
        }

        holderIds.getManager().removeFromUniqueDataCollection(holderIds, ids);

        return !ids.isEmpty();
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

        holderIds.getManager().removeFromUniqueDataCollection(holderIds, toRemove);

        return !toRemove.isEmpty();
    }

    @Override
    public void clear() {
        List<UUID> ids = new ArrayList<>(holderIds);
        holderIds.getManager().removeFromUniqueDataCollection(holderIds, ids);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" +
                "dataType=" + getDataType() +
                ", holderIds=" + holderIds +
                '}';
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

        public T next() {
            int i = cursor;
            if (!hasNext())
                throw new NoSuchElementException();
            cursor = i + 1;
            UUID id = ids[lastRet = i];
            return getDataManager().getUniqueData(getDataType(), id);
        }

        public void remove() {
            if (lastRet < 0)
                throw new IllegalStateException();
            UUID id = ids[lastRet];
            holderIds.getManager().removeFromUniqueDataCollection(holderIds, Collections.singletonList(id));

            lastRet = -1;
        }

        @Override
        public void forEachRemaining(Consumer<? super T> action) {
            Objects.requireNonNull(action);
            for (int i = cursor; i < ids.length; i++) {
                UUID id = ids[i];
                action.accept(getDataManager().getUniqueData(getDataType(), id));
            }
            cursor = ids.length;
        }
    }
}
