package net.staticstudios.data.util;

import net.staticstudios.data.UniqueData;

import java.util.function.Supplier;

public class CachedValueUpdateHandlerWrapper<U extends UniqueData, T> extends ValueUpdateHandlerWrapper<U, T> {
    private final Supplier<T> fallbackSupplier;

    public CachedValueUpdateHandlerWrapper(ValueUpdateHandler<U, T> handler, Class<T> dataType, Class<? extends UniqueData> holderClass, Supplier<T> fallbackSupplier) {
        super(handler, dataType, holderClass);
        this.fallbackSupplier = fallbackSupplier;
    }

    public T getFallback() {
        return fallbackSupplier.get();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        CachedValueUpdateHandlerWrapper<?, ?> that = (CachedValueUpdateHandlerWrapper<?, ?>) obj;
        return super.equals(that);
    }

    @Override
    public String toString() {
        return "CachedValueUpdateHandlerWrapper{" +
                "handler=" + getHandler() +
                ", dataType=" + getDataType() +
                ", holderClass=" + getHolderClass() +
                '}';
    }
}
