package net.staticstudios.data.data;

public interface Value<T> extends Data<T> {
    T get();

    void set(T value);
}
