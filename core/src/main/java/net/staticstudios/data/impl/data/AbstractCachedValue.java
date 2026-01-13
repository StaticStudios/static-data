package net.staticstudios.data.impl.data;

import com.google.common.base.Preconditions;
import net.staticstudios.data.CachedValue;

import java.util.function.Supplier;

public abstract class AbstractCachedValue<T> implements CachedValue<T> {
    private Supplier<T> fallbackSupplier;
    private Supplier<T> refreshSupplier;

    public void setFallback(Supplier<T> fallbackSupplier) {
        this.fallbackSupplier = fallbackSupplier;
    }

    protected T getFallback() {
        if (fallbackSupplier != null) {
            return fallbackSupplier.get();
        }
        return null;
    }

    public void setRefresh(Supplier<T> refreshSupplier) {
        this.refreshSupplier = refreshSupplier;
    }

    protected T refresh() {
        if (refreshSupplier == null) {
            return null;
        }
        T value = refreshSupplier.get();
        set(value);
        return value;
    }
}
