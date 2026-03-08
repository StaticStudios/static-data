package net.staticstudios.data.util;

import net.staticstudios.data.UniqueData;

import java.util.Objects;

public class ValueUpdateHandlerWrapper<U extends UniqueData, T> {
    private final ValueUpdateHandler<U, T> handler;
    private final Class<T> dataType;
    private final Class<? extends UniqueData> holderClass;

    public ValueUpdateHandlerWrapper(ValueUpdateHandler<U, T> handler, Class<T> dataType, Class<? extends UniqueData> holderClass) {
        this.handler = handler;
        this.dataType = dataType;
        this.holderClass = holderClass;
    }


    public ValueUpdateHandler<U, T> getHandler() {
        return handler;
    }

    public Class<T> getDataType() {
        return dataType;
    }

    public Class<? extends UniqueData> getHolderClass() {
        return holderClass;
    }

    public void unsafeHandle(UniqueData holder, Object oldValue, Object newValue) {
        handler.unsafeHandle(holder, oldValue, newValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(handler, dataType, holderClass);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        ValueUpdateHandlerWrapper<?, ?> that = (ValueUpdateHandlerWrapper<?, ?>) obj;
        return dataType.equals(that.dataType) && holderClass.equals(that.holderClass) && handler.equals(that.handler);
    }
}
