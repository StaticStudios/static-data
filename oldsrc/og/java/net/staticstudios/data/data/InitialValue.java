package net.staticstudios.data.data;

import net.staticstudios.data.data.value.Value;

public interface InitialValue<V extends Value<?>, T> {
    V getValue();

    T getInitialDataValue();
}
