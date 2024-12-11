package net.staticstudios.data.data.collection;

import net.staticstudios.data.data.DataHolder;
import net.staticstudios.data.data.UniqueData;
import org.jetbrains.annotations.NotNull;

import java.util.*;

public class PersistentUniqueDataCollection<T extends UniqueData> extends PersistentCollection<T> {
    private final PersistentValueCollection<UUID> holderIds;

    public PersistentUniqueDataCollection(DataHolder holder, String schema, String table, String linkingColumn) {
        super(holder, schema, table, linkingColumn, linkingColumn);
        holderIds = new PersistentValueCollection<>(holder, schema, table, linkingColumn, linkingColumn);
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
    public @NotNull Iterator<T> iterator() { //todo: this
        return null;
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
        holderIds.getManager().addEntries(holderIds, Collections.singletonList(new CollectionEntry(t.getId(), getRootHolder().getId())));
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
}
