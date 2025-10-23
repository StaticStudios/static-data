package net.staticstudios.data.util;

import net.staticstudios.data.UniqueData;

public interface ValueUpdateHandler<U extends UniqueData, T> {

    void handle(U holder, ValueUpdate<T> update);

    @SuppressWarnings("unchecked")
    default void unsafeHandle(UniqueData holder, Object oldValue, Object newValue) {
        handle((U) holder, new ValueUpdate<>((T) oldValue, (T) newValue));
    }
}
