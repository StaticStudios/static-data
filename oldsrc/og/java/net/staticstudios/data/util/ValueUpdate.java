package net.staticstudios.data.util;

import org.jetbrains.annotations.Nullable;

public record ValueUpdate<T>(@Nullable T oldValue, @Nullable T newValue) {
}
