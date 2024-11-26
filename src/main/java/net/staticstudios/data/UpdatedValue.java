package net.staticstudios.data;

public record UpdatedValue<T>(T oldValue, T newValue) {
    public static <T> UpdatedValue<T> of(T oldValue, T newValue) {
        return new UpdatedValue<>(oldValue, newValue);
    }
}
