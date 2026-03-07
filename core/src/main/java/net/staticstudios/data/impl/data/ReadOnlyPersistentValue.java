package net.staticstudios.data.impl.data;

import net.staticstudios.data.PersistentValue;
import net.staticstudios.data.UniqueData;
import net.staticstudios.data.util.PersistentValueMetadata;
import net.staticstudios.data.util.ReflectionUtils;
import net.staticstudios.data.util.UniqueDataMetadata;
import net.staticstudios.data.util.ValueUpdateHandler;

import java.lang.reflect.Field;

public class ReadOnlyPersistentValue<T> implements PersistentValue<T> {
    private final T value;
    private final UniqueData holder;
    private final Class<T> dataType;

    public ReadOnlyPersistentValue(UniqueData holder, Class<T> dataType, T value) {
        this.holder = holder;
        this.dataType = dataType;
        this.value = holder.getDataManager().copy(value, dataType);
    }

    private static <T> void createAndDelegate(PersistentValue.ProxyPersistentValue<T> proxy, PersistentValueMetadata metadata) {
        ReadOnlyPersistentValue<T> delegate = new ReadOnlyPersistentValue<>(
                proxy.getHolder(),
                proxy.getDataType(),
                PersistentValueImpl.create(proxy.getHolder(), proxy.getDataType(), metadata).get()
        );

        proxy.setDelegate(metadata, delegate);
    }

    private static <T> PersistentValue<T> create(UniqueData holder, Class<T> dataType, PersistentValueMetadata metadata) {
        return new ReadOnlyPersistentValue<>(holder, dataType, PersistentValueImpl.create(holder, dataType, metadata).get());
    }

    public static <U extends UniqueData> void delegate(U instance) {
        UniqueDataMetadata metadata = instance.getDataManager().getMetadata(instance.getClass());
        try {
            for (var entry : metadata.persistentValueMetadata().entrySet()) {
                Field field = entry.getKey();
                PersistentValueMetadata pvMetadata = entry.getValue();

                Object value = field.get(instance);
                if (value instanceof PersistentValue.ProxyPersistentValue<?> proxyPv) {
                    ReadOnlyPersistentValue.createAndDelegate(proxyPv, pvMetadata);
                } else {
                    field.set(instance, ReadOnlyPersistentValue.create(instance, ReflectionUtils.getGenericType(field), pvMetadata));
                }
            }
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
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
    public <U extends UniqueData> PersistentValue<T> onUpdate(Class<U> holderClass, ValueUpdateHandler<U, T> updateHandler) {
        throw new UnsupportedOperationException("Read-only value cannot have update handlers");
    }

    @Override
    public T get() {
        return value;
    }

    @Override
    public void set(T value) {
        throw new UnsupportedOperationException("Cannot set value on a read-only PersistentValue");
    }

    @Override
    public String toString() {
        return "ReadOnlyPersistentValue{" +
                "value=" + value +
                '}';
    }
}
