package net.staticstudios.data.impl.data;

import net.staticstudios.data.PersistentCollection;
import net.staticstudios.data.UniqueData;
import net.staticstudios.data.util.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;

public class ReadOnlyValuedCollection<T> implements PersistentCollection<T> {
    private final Collection<T> values;
    private final UniqueData holder;

    public ReadOnlyValuedCollection(UniqueData holder, Class<T> valueType, Collection<T> values) {
        this.holder = holder;
        List<T> copy = new ArrayList<>();
        for (T value : values) {
            copy.add(holder.getDataManager().copy(value, valueType));
        }
        this.values = Collections.unmodifiableCollection(copy);
    }

    private static <T> void createAndDelegate(PersistentCollection.ProxyPersistentCollection<T> proxy, PersistentOneToManyValueCollectionMetadata metadata) {
        ReadOnlyValuedCollection<T> delegate = new ReadOnlyValuedCollection<>(
                proxy.getHolder(),
                proxy.getDataType(),
                PersistentOneToManyValueCollectionImpl.create(proxy.getHolder(), proxy.getDataType(), metadata.getDataSchema(), metadata.getDataTable(), metadata.getDataColumn(), metadata.getLinks())
        );

        proxy.setDelegate(metadata, delegate);
    }

    private static <T> ReadOnlyValuedCollection<T> create(UniqueData holder, Class<T> dataType, PersistentOneToManyValueCollectionMetadata metadata) {
        return new ReadOnlyValuedCollection<>(holder, dataType, PersistentOneToManyValueCollectionImpl.create(holder, dataType, metadata.getDataSchema(), metadata.getDataTable(), metadata.getDataColumn(), metadata.getLinks()));
    }

    public static <U extends UniqueData> void delegate(U instance) {
        UniqueDataMetadata metadata = instance.getDataManager().getMetadata(instance.getClass());
        for (FieldInstancePair<@Nullable PersistentCollection> pair : ReflectionUtils.getFieldInstancePairs(instance, PersistentCollection.class)) {
            PersistentCollectionMetadata collectionMetadata = metadata.persistentCollectionMetadata().get(pair.field());
            if (!(collectionMetadata instanceof PersistentOneToManyValueCollectionMetadata oneToManyValueMetadata))
                continue;

            if (pair.instance() instanceof PersistentCollection.ProxyPersistentCollection<?> proxyPv) {
                createAndDelegate(proxyPv, oneToManyValueMetadata);
            } else {
                pair.field().setAccessible(true);
                try {
                    pair.field().set(instance, create(instance, ReflectionUtils.getGenericType(pair.field()), oneToManyValueMetadata));
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
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
        return values.size();
    }

    @Override
    public boolean isEmpty() {
        return values.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return values.contains(o);
    }

    @Override
    public @NotNull Iterator<T> iterator() {
        return values.iterator();
    }

    @Override
    public @NotNull Object[] toArray() {
        return values.toArray();
    }

    @Override
    public @NotNull <T1> T1[] toArray(@NotNull T1[] a) {
        return values.toArray(a);
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
        return values.containsAll(c);
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
        return values.toString();
    }
}
