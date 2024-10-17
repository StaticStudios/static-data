package net.staticstudios.data;

public record PersistentValueChange<T>(T oldValue, T newValue) {
}
