package net.staticstudios.data;

public interface Value<T> {
    T get();

    void set(T value);
}
