package net.staticstudios.data.util;

public interface Value<T> {
    T get();

    void set(T value);
}