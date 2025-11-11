package net.staticstudios.data.util;

import net.staticstudios.data.UniqueData;

import java.util.Objects;

public class CollectionChangeHandlerWrapper<U extends UniqueData, T> {
    private final CollectionChangeHandler<U, T> handler;
    private final Class<T> dataType;
    private final Class<? extends UniqueData> holderClass;
    private final Type type;
    private PersistentCollectionMetadata collectionMetadata;

    public CollectionChangeHandlerWrapper(CollectionChangeHandler<U, T> handler, Class<T> dataType, Class<? extends UniqueData> holderClass, Type type) {
        LambdaUtils.assertLambdaDoesntCapture(handler, "Use thr provided instance to access member variables.");
        // we don't want to hold a reference to a UniqueData instances, since it won't get GCed
        // and the handler may be called for any holder instance.

        this.handler = handler;
        this.dataType = dataType;
        this.holderClass = holderClass;
        this.type = type;
    }


    public CollectionChangeHandler<U, T> getHandler() {
        return handler;
    }

    public Class<T> getDataType() {
        return dataType;
    }

    public Class<? extends UniqueData> getHolderClass() {
        return holderClass;
    }

    public void unsafeHandle(UniqueData holder, Object value) {
        handler.unsafeHandle(holder, value);
    }

    public Type getType() {
        return type;
    }

    public void setCollectionMetadata(PersistentCollectionMetadata collectionMetadata) {
        this.collectionMetadata = collectionMetadata;
    }

    public PersistentCollectionMetadata getCollectionMetadata() {
        return collectionMetadata;
    }

    @Override
    public int hashCode() {
        return Objects.hash(handler, dataType, holderClass, type);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        CollectionChangeHandlerWrapper<?, ?> that = (CollectionChangeHandlerWrapper<?, ?>) obj;
        return dataType.equals(that.dataType) && holderClass.equals(that.holderClass) && handler.equals(that.handler) && type == that.type;
    }

    public enum Type {
        ADD,
        REMOVE
    }
}
