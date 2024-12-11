package net.staticstudios.data.data;

public interface InitialData<K extends Keyed, V> {
    K getKeyed();

    V getValue();
}
