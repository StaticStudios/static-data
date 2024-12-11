package net.staticstudios.data.v2;

public interface InitialData<D extends Data<?>> {
    D getData();

    Object getValue();
}
