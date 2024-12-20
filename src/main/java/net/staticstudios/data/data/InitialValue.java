package net.staticstudios.data.data;

public interface InitialValue<V extends Value<?>, T> {
    V getValue();

    T getInitialDataValue();
}
