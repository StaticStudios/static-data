package net.staticstudios.data;

public record ValueUpdate<T>(T oldValue, T newValue) {
}
