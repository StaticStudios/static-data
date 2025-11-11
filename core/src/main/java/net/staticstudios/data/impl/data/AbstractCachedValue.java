package net.staticstudios.data.impl.data;

import net.staticstudios.data.CachedValue;

import java.util.function.Supplier;

public abstract class AbstractCachedValue<T> implements CachedValue<T> {
    private Supplier<T> fallbackSupplier;

    public void setFallback(Supplier<T> fallbackSupplier) {
        this.fallbackSupplier = fallbackSupplier;
    }

    protected T getFallback() {
        if (fallbackSupplier != null) {
            return fallbackSupplier.get();
        }
        return null;
    }
}
