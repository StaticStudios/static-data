package net.staticstudios.data.impl.data;

import net.staticstudios.data.CachedValue;
import net.staticstudios.data.UniqueData;
import net.staticstudios.data.util.CachedValueRefresher;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.Nullable;

import java.util.function.Supplier;

public abstract class AbstractCachedValue<T> implements CachedValue<T> {
    private Supplier<T> fallbackSupplier;
    private CachedValueRefresher<UniqueData, T> refreshFunction;

    @ApiStatus.Internal
    public void setFallback(Supplier<T> fallbackSupplier) {
        this.fallbackSupplier = fallbackSupplier;
    }

    @ApiStatus.Internal
    public void setRefresher(CachedValueRefresher<UniqueData, T> refreshFunction) {
        this.refreshFunction = refreshFunction;
    }

    protected T getFallback() {
        if (fallbackSupplier != null) {
            return fallbackSupplier.get();
        }
        return null;
    }

    protected T calculateRefreshedValue(@Nullable T currentValue) {
        if (refreshFunction != null) {
            return refreshFunction.apply(getHolder(), currentValue);
        }
        return null;
    }
}
