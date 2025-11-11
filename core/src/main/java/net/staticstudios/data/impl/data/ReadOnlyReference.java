package net.staticstudios.data.impl.data;

import net.staticstudios.data.Reference;
import net.staticstudios.data.UniqueData;
import net.staticstudios.data.util.*;
import org.jetbrains.annotations.Nullable;

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
                ReferenceImpl.create(proxy.getHolder(), proxy.getReferenceType(), metadata.getLinks()).getReferencedColumnValuePairs()
        );

        proxy.setDelegate(delegate);
    }

    private static <T extends UniqueData> Reference<T> create(UniqueData holder, Class<T> referenceType, ReferenceMetadata metadata) {
        return new ReadOnlyReference<>(holder, referenceType, ReferenceImpl.create(holder, referenceType, metadata.getLinks()).getReferencedColumnValuePairs());
    }

    public static <U extends UniqueData> void delegate(U instance) {
        UniqueDataMetadata metadata = instance.getDataManager().getMetadata(instance.getClass());
        for (FieldInstancePair<@Nullable Reference> pair : ReflectionUtils.getFieldInstancePairs(instance, Reference.class)) {
            ReferenceMetadata refMetadata = metadata.referenceMetadata().get(pair.field());
            if (pair.instance() instanceof Reference.ProxyReference<?> proxyRef) {
                createAndDelegate(proxyRef, refMetadata);
            } else {
                pair.field().setAccessible(true);
                try {
                    pair.field().set(instance, create(instance, refMetadata.getReferencedClass(), refMetadata));
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
    public Class<T> getReferenceType() {
        return referenceType;
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
