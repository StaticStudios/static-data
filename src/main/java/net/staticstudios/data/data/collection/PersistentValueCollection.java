package net.staticstudios.data.data.collection;

import net.staticstudios.data.data.DataHolder;
import net.staticstudios.data.impl.PersistentCollectionValueManager;
import org.jetbrains.annotations.NotNull;

import java.util.*;

public class PersistentValueCollection<T> extends PersistentCollection<T> {

    public PersistentValueCollection(DataHolder holder, String schema, String table, String linkingColumn, String dataColumn) {
        super(holder, schema, table, linkingColumn, dataColumn);
    }

    @Override
    public int size() {
        return getManager().getEntryKeys(this).size();
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean contains(Object o) {
        return getInternalValues().contains(o);
    }

    @Override
    public @NotNull Iterator<T> iterator() {
        //todo: this
        return null;
    }

    @Override
    public @NotNull Object[] toArray() {
        return getInternalValues().toArray();
    }

    @Override
    public @NotNull <T1> T1[] toArray(@NotNull T1[] a) {
        return getInternalValues().toArray(a);
    }

    @Override
    public boolean add(T t) {
        getManager().addEntries(this, Collections.singletonList(new CollectionEntry(UUID.randomUUID(), t)));
        return true;
    }

    @Override
    public boolean remove(Object o) {
        PersistentCollectionValueManager manager = getManager();
        Collection<KeyedEntry> keyedEntries = manager.getKeyedEntries(this);
        for (KeyedEntry keyedEntry : keyedEntries) {
            if (keyedEntry.value().equals(o)) {
                manager.removeEntry(this, keyedEntry);
                return true;
            }
        }

        return false;
    }

    @Override
    public boolean containsAll(@NotNull Collection<?> c) {
        return getInternalValues().containsAll(c);
    }

    @Override
    public boolean addAll(@NotNull Collection<? extends T> c) {
        getManager().addEntries(this, c.stream().map(value -> new CollectionEntry(UUID.randomUUID(), value)).toList());
        return true;
    }

    @Override
    public boolean removeAll(@NotNull Collection<?> c) {
        boolean changed = false;

        PersistentCollectionValueManager manager = getManager();
        Collection<KeyedEntry> keyedEntries = manager.getKeyedEntries(this);
        List<KeyedEntry> toRemove = new ArrayList<>();
        for (Object o : c) {
            for (KeyedEntry keyedEntry : keyedEntries) {
                if (keyedEntry.value().equals(o)) {
                    keyedEntries.remove(keyedEntry);
                    toRemove.add(keyedEntry);
                    changed = true;
                    break;
                }
            }
        }

        manager.removeEntries(this, toRemove);

        return changed;
    }

    @Override
    public boolean retainAll(@NotNull Collection<?> c) {
        boolean changed = false;

        PersistentCollectionValueManager manager = getManager();
        Collection<KeyedEntry> keyedEntries = manager.getKeyedEntries(this);
        List<KeyedEntry> toRemove = new ArrayList<>();
        for (KeyedEntry keyedEntry : keyedEntries) {
            if (!c.contains(keyedEntry.value())) {
                toRemove.add(keyedEntry);
                changed = true;
            }
        }

        manager.removeEntries(this, toRemove);

        return changed;
    }

    @Override
    public void clear() {
        getManager().removeEntries(this, getManager().getKeyedEntries(this));
    }

    protected PersistentCollectionValueManager getManager() {
        return getDataManager().getDataTypeManager(PersistentCollectionValueManager.class);
    }

    @SuppressWarnings("unchecked")
    private Collection<T> getInternalValues() {
        return (Collection<T>) getManager().getEntries(this);
    }
    
    //todo: hashcode and equals
}
