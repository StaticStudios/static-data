package net.staticstudios.data.data.value;

import net.staticstudios.data.data.Data;

public interface Value<T> extends Data<T> {
    T get();

    void set(T value);
}
