package net.staticstudios.data;

import org.jetbrains.annotations.Nullable;

public record ValueUpdate<T>(@Nullable T oldValue, @Nullable T newValue) {
}
