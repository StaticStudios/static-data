package net.staticstudios.data;

public interface ValueUpdateHandler<T> {

    void handle(ValueUpdate<T> update);

    @SuppressWarnings("unchecked")
    default void unsafeHandle(Object oldValue, Object newValue) {
        handle(new ValueUpdate<>((T) oldValue, (T) newValue));
    }
}
