package net.staticstudios.data.impl.data;

import net.staticstudios.data.CachedValue;
import net.staticstudios.data.UniqueData;
import net.staticstudios.data.util.*;
import org.jetbrains.annotations.Nullable;

import java.util.function.Supplier;

public class ReadOnlyCachedValue<T> extends AbstractCachedValue<T> {
    private final T value;
    private final UniqueData holder;
    private final Class<T> dataType;

    public ReadOnlyCachedValue(UniqueData holder, Class<T> dataType, T value) {
        this.holder = holder;
        this.dataType = dataType;
        this.value = holder.getDataManager().copy(value, dataType);
    }

    private static <T> void createAndDelegate(CachedValue.ProxyCachedValue<T> proxy, CachedValueMetadata metadata) {
        ReadOnlyCachedValue<T> delegate = new ReadOnlyCachedValue<>(
                proxy.getHolder(),
                proxy.getDataType(),
                CachedValueImpl.create(proxy.getHolder(), proxy.getDataType(), metadata).get()
        );

        proxy.setDelegate(metadata, delegate);
    }

    private static <T> CachedValue<T> create(UniqueData holder, Class<T> dataType, CachedValueMetadata metadata) {
        return new ReadOnlyCachedValue<>(holder, dataType, CachedValueImpl.create(holder, dataType, metadata).get());
    }

    public static <U extends UniqueData> void delegate(U instance) {
        UniqueDataMetadata metadata = instance.getDataManager().getMetadata(instance.getClass());
        for (FieldInstancePair<@Nullable CachedValue> pair : ReflectionUtils.getFieldInstancePairs(instance, CachedValue.class)) {
            CachedValueMetadata cvMetadata = metadata.cachedValueMetadata().get(pair.field());
            if (pair.instance() instanceof CachedValue.ProxyCachedValue<?> proxyPv) {
                createAndDelegate(proxyPv, cvMetadata);
            } else {
                pair.field().setAccessible(true);
                try {
                    pair.field().set(instance, create(instance, ReflectionUtils.getGenericType(pair.field()), cvMetadata));
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    @Override
    public UniqueData getHolder() {
        return holder;
    }

    @Override
    public Class<T> getDataType() {
        return dataType;
    }

    @Override
    public <U extends UniqueData> CachedValue<T> onUpdate(Class<U> holderClass, ValueUpdateHandler<U, T> updateHandler) {
        throw new UnsupportedOperationException("Read-only value cannot have update handlers");
    }

    @Override
    public CachedValue<T> supplyFallback(Supplier<T> fallback) {
        throw new UnsupportedOperationException("Cannot set fallback on a read-only CachedValue");
    }

    @Override
    public <U extends UniqueData> CachedValue<T> refresher(Class<U> clazz, CachedValueRefresher<U, T> refresher) {
        throw new UnsupportedOperationException("Cannot set refresher on a read-only CachedValue");
    }

    @Override
    public T get() {
        return value;
    }

    @Override
    public void set(T value) {
        throw new UnsupportedOperationException("Cannot set value on a read-only CachedValue");
    }

    @Override
    public @Nullable T refresh() {
        return value;
    }

    @Override
    public String toString() {
        return "ReadOnlyCachedValue{" +
                "value=" + value +
                '}';
    }
}
