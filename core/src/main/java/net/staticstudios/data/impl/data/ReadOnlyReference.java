package net.staticstudios.data.impl.data;

import net.staticstudios.data.Reference;
import net.staticstudios.data.UniqueData;
import net.staticstudios.data.util.ColumnValuePairs;
import net.staticstudios.data.util.ReferenceMetadata;
import net.staticstudios.data.util.ReferenceUpdateHandler;
import net.staticstudios.data.util.UniqueDataMetadata;

import java.lang.reflect.Field;

public class ReadOnlyReference<T extends UniqueData> implements Reference<T> {
    private final ColumnValuePairs referencedColumnValuePairs;
    private final UniqueData holder;
    private final Class<T> referenceType;

    public ReadOnlyReference(UniqueData holder, Class<T> referenceType, ColumnValuePairs referencedColumnValuePairs) {
        this.holder = holder;
        this.referenceType = referenceType;
        this.referencedColumnValuePairs = referencedColumnValuePairs;
    }

    private static <T extends UniqueData> void createAndDelegate(Reference.ProxyReference<T> proxy, ReferenceMetadata metadata) {
        ReadOnlyReference<T> delegate = new ReadOnlyReference<>(
                proxy.getHolder(),
                proxy.getReferenceType(),
                ReferenceImpl.create(proxy.getHolder(), proxy.getReferenceType(), metadata).getReferencedColumnValuePairs()
        );

        proxy.setDelegate(metadata, delegate);
    }

    private static <T extends UniqueData> Reference<T> create(UniqueData holder, Class<T> referenceType, ReferenceMetadata metadata) {
        return new ReadOnlyReference<>(holder, referenceType, ReferenceImpl.create(holder, referenceType, metadata).getReferencedColumnValuePairs());
    }

    public static <U extends UniqueData> void delegate(U instance) {
        UniqueDataMetadata metadata = instance.getDataManager().getMetadata(instance.getClass());
        try {
            for (var entry : metadata.referenceMetadata().entrySet()) {
                Field field = entry.getKey();
                ReferenceMetadata referenceMetadata = entry.getValue();

                Object value = field.get(instance);
                if (value instanceof Reference.ProxyReference<?> proxyRef) {
                    ReadOnlyReference.createAndDelegate(proxyRef, referenceMetadata);
                } else {
                    field.set(instance, ReadOnlyReference.create(instance, referenceMetadata.referencedClass(), referenceMetadata));
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
    public Class<T> getReferenceType() {
        return referenceType;
    }

    @Override
    public <U extends UniqueData> Reference<T> onUpdate(Class<U> holderClass, ReferenceUpdateHandler<U, T> updateHandler) {
        throw new UnsupportedOperationException("Read-only value cannot have update handlers");
    }

    @Override
    public T get() {
        return holder.getDataManager().getInstance(referenceType, referencedColumnValuePairs.getPairs());
    }

    @Override
    public void set(T value) {
        throw new UnsupportedOperationException("Cannot set value on a read-only Reference");
    }

    @Override
    public String toString() {
        return "ReadOnlyReference{" +
                "holder=" + holder +
                ", referenceType=" + referenceType +
                ", referencedColumnValuePairs=" + referencedColumnValuePairs +
                '}';
    }
}
