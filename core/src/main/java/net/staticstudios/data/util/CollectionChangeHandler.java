package net.staticstudios.data.util;

import net.staticstudios.data.UniqueData;

public interface CollectionChangeHandler<U extends UniqueData, T> {

    void handle(U holder, T value);

    @SuppressWarnings("unchecked")
    default void unsafeHandle(UniqueData holder, Object value) {
        handle((U) holder, (T) value);
    }
}
