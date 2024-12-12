package net.staticstudios.data.data;

public interface SingleData<T> extends Data<T> {
    T get();

    void set(T value);
}
