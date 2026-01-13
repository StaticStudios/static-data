package net.staticstudios.data.util;

import net.staticstudios.data.UniqueData;
import org.jetbrains.annotations.Nullable;

public interface CachedValueRefresher<U extends UniqueData, T> {
    T apply(U holder, @Nullable T previousValue);
}
