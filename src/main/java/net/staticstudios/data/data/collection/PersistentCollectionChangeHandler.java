package net.staticstudios.data.data.collection;

public interface PersistentCollectionChangeHandler<T> {
    void onChange(T obj);

    @SuppressWarnings("unchecked")
    default void unsafeOnChange(Object obj) {
        onChange((T) obj);
    }
}
