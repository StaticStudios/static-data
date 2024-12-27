package net.staticstudios.data.data.collection;

public interface PersistentCollectionChangeHandler<T> {
    void onChange(T obj);
}
