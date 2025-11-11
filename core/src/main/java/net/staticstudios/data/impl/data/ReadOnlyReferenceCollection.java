package net.staticstudios.data.impl.data;

import net.staticstudios.data.PersistentCollection;
import net.staticstudios.data.UniqueData;
import net.staticstudios.data.util.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Iterator;

public class ReadOnlyReferenceCollection<T extends UniqueData> implements PersistentCollection<T> {
    private final Collection<ColumnValuePairs> referencedColumnValuePairsCollection;
    private final UniqueData holder;
    private final Class<T> referenceType;

    public ReadOnlyReferenceCollection(UniqueData holder, Class<T> referenceType, Collection<ColumnValuePairs> referencedColumnValuePairsCollection) {
        this.holder = holder;
        this.referenceType = referenceType;
        this.referencedColumnValuePairsCollection = referencedColumnValuePairsCollection;
    }

    private static <T extends UniqueData> void createAndDelegate(PersistentCollection.ProxyPersistentCollection<T> proxy, PersistentOneToManyCollectionMetadata metadata) {
        ReadOnlyReferenceCollection<T> delegate = new ReadOnlyReferenceCollection<>(
                proxy.getHolder(),
                proxy.getDataType(),
                PersistentOneToManyCollectionImpl.create(proxy.getHolder(), proxy.getDataType(), metadata.getLinks()).getIds()
        );

        proxy.setDelegate(metadata, delegate);
    }

    private static <T extends UniqueData> void createAndDelegate(PersistentCollection.ProxyPersistentCollection<T> proxy, PersistentManyToManyCollectionMetadata metadata) {
        ReadOnlyReferenceCollection<T> delegate = new ReadOnlyReferenceCollection<>(
                proxy.getHolder(),
                proxy.getDataType(),
                PersistentManyToManyCollectionImpl.create(proxy.getHolder(), metadata).getIds()
        );

        proxy.setDelegate(metadata, delegate);
    }

    private static <T extends UniqueData> ReadOnlyReferenceCollection<T> create(UniqueData holder, Class<T> dataType, PersistentOneToManyCollectionMetadata metadata) {
        return new ReadOnlyReferenceCollection<>(holder, dataType, PersistentOneToManyCollectionImpl.create(holder, dataType, metadata.getLinks()).getIds());
    }

    private static <T extends UniqueData> ReadOnlyReferenceCollection<T> create(UniqueData holder, Class<T> dataType, PersistentManyToManyCollectionMetadata metadata) {
        return new ReadOnlyReferenceCollection<>(holder, dataType, PersistentManyToManyCollectionImpl.create(holder, metadata).getIds());
    }

    public static <U extends UniqueData> void delegate(U instance) {
        UniqueDataMetadata metadata = instance.getDataManager().getMetadata(instance.getClass());
        for (FieldInstancePair<@Nullable PersistentCollection> pair : ReflectionUtils.getFieldInstancePairs(instance, PersistentCollection.class)) {
            PersistentCollectionMetadata collectionMetadata = metadata.persistentCollectionMetadata().get(pair.field());
            if (collectionMetadata instanceof PersistentOneToManyCollectionMetadata oneToManyMetadata) {
                if (pair.instance() instanceof PersistentCollection.ProxyPersistentCollection proxyCollection) {
                    createAndDelegate(proxyCollection, oneToManyMetadata);
                } else {
                    pair.field().setAccessible(true);
                    try {
                        pair.field().set(instance, create(instance, oneToManyMetadata.getReferencedType(), oneToManyMetadata));
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }
                }
            } else if (collectionMetadata instanceof PersistentManyToManyCollectionMetadata manyToManyMetadata) {
                if (pair.instance() instanceof PersistentCollection.ProxyPersistentCollection proxyPv) {
                    createAndDelegate(proxyPv, manyToManyMetadata);
                } else {
                    pair.field().setAccessible(true);
                    try {
                        pair.field().set(instance, create(instance, manyToManyMetadata.getReferencedType(), manyToManyMetadata));
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }

    @Override
    public <U extends UniqueData> PersistentCollection<T> onAdd(Class<U> holderClass, CollectionChangeHandler<U, T> addHandler) {
        throw new UnsupportedOperationException("Read-only collection cannot have add handlers");
    }

    @Override
    public <U extends UniqueData> PersistentCollection<T> onRemove(Class<U> holderClass, CollectionChangeHandler<U, T> removeHandler) {
        throw new UnsupportedOperationException("Read-only collection cannot have add handlers");
    }

    @Override
    public UniqueData getHolder() {
        return holder;
    }

    @Override
    public int size() {
        return referencedColumnValuePairsCollection.size();
    }

    @Override
    public boolean isEmpty() {
        return referencedColumnValuePairsCollection.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        if (!(o instanceof UniqueData data)) {
            return false;
        }
        return referencedColumnValuePairsCollection.contains(data.getIdColumns());
    }

    @Override
    public @NotNull Iterator<T> iterator() {
        return new IteratorImpl<>(holder, referenceType, referencedColumnValuePairsCollection.iterator());
    }

    @Override
    public @NotNull Object[] toArray() {
        Object[] array = new Object[referencedColumnValuePairsCollection.size()];
        int index = 0;
        for (ColumnValuePairs pairs : referencedColumnValuePairsCollection) {
            array[index++] = holder.getDataManager().getInstance(referenceType, pairs.getPairs());
        }
        return array;
    }

    @Override
    public @NotNull <T1> T1[] toArray(@NotNull T1[] a) {
        if (a.length < referencedColumnValuePairsCollection.size()) {
            a = (T1[]) Array.newInstance(a.getClass().getComponentType(), referencedColumnValuePairsCollection.size());
        }
        int index = 0;
        for (ColumnValuePairs pairs : referencedColumnValuePairsCollection) {
            a[index++] = (T1) holder.getDataManager().getInstance(referenceType, pairs.getPairs());
        }
        if (a.length > referencedColumnValuePairsCollection.size()) {
            a[referencedColumnValuePairsCollection.size()] = null;
        }
        return a;
    }

    @Override
    public boolean add(T t) {
        throw new UnsupportedOperationException("Cannot add to a read-only collection");
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException("Cannot remove from a read-only collection");
    }

    @Override
    public boolean containsAll(@NotNull Collection<?> c) {
        for (Object o : c) {
            if (!contains(o)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean addAll(@NotNull Collection<? extends T> c) {
        throw new UnsupportedOperationException("Cannot add to a read-only collection");
    }

    @Override
    public boolean removeAll(@NotNull Collection<?> c) {
        throw new UnsupportedOperationException("Cannot remove from a read-only collection");
    }

    @Override
    public boolean retainAll(@NotNull Collection<?> c) {
        throw new UnsupportedOperationException("Cannot retain on a read-only collection");
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("Cannot clear a read-only collection");
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("[");
        Iterator<T> iterator = iterator();
        while (iterator.hasNext()) {
            T item = iterator.next();
            sb.append(item);
            if (iterator.hasNext()) {
                sb.append(", ");
            }
        }
        sb.append("]");
        return sb.toString();
    }

    static class IteratorImpl<T extends UniqueData> implements Iterator<T> {
        private final Iterator<ColumnValuePairs> internalIterator;
        private final UniqueData holder;
        private final Class<T> valueType;

        public IteratorImpl(UniqueData holder, Class<T> valueType, Iterator<ColumnValuePairs> internalIterator) {
            this.holder = holder;
            this.valueType = valueType;
            this.internalIterator = internalIterator;
        }

        @Override
        public boolean hasNext() {
            return internalIterator.hasNext();
        }

        @Override
        public T next() {
            ColumnValuePairs pairs = internalIterator.next();
            return holder.getDataManager().getInstance(valueType, pairs.getPairs());
        }
    }
}
