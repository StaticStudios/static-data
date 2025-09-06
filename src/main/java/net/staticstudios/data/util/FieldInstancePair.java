package net.staticstudios.data.util;

import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Field;

public record FieldInstancePair<T>(@NotNull Field field, T instance) {
}
