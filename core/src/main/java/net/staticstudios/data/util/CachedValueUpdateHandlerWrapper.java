package net.staticstudios.data.util;

import net.staticstudios.data.UniqueData;

public class CachedValueUpdateHandlerWrapper<U extends UniqueData, T> extends ValueUpdateHandlerWrapper<U, T> {
    private final T fallback;

    public CachedValueUpdateHandlerWrapper(ValueUpdateHandler<U, T> handler, Class<T> dataType, Class<? extends UniqueData> holderClass, T fallback) {
        super(handler, dataType, holderClass);
        this.fallback = fallback;
    }

    public T getFallback() {
        return fallback;
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
                "fallback=" + fallback +
                '}';
    }
}
