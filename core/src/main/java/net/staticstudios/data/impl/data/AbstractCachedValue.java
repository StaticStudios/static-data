package net.staticstudios.data.impl.data;

import net.staticstudios.data.CachedValue;
import net.staticstudios.data.UniqueData;
import net.staticstudios.data.util.CachedValueRefresher;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.Nullable;

public abstract class AbstractCachedValue<T> implements CachedValue<T> {
    private T fallback;
    private CachedValueRefresher<UniqueData, T> refreshFunction;

    @ApiStatus.Internal
    public void setFallback(T fallback) {
        this.fallback = fallback;
    }

    @ApiStatus.Internal
    public void setRefresher(CachedValueRefresher<UniqueData, T> refreshFunction) {
        this.refreshFunction = refreshFunction;
    }

    protected T getFallback() {
        return fallback;
    }

    protected T calculateRefreshedValue(@Nullable T currentValue) {
        if (refreshFunction != null) {
            return refreshFunction.apply(getHolder(), currentValue);
        }
        return null;
    }
}
