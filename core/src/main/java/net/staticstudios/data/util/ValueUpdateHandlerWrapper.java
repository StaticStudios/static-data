package net.staticstudios.data.util;

import net.staticstudios.data.UniqueData;

import java.util.Objects;

public class ValueUpdateHandlerWrapper<U extends UniqueData, T> {
    private final ValueUpdateHandler<U, T> handler;
    private final Class<T> dataType;
    private final Class<? extends UniqueData> holderClass;

    public ValueUpdateHandlerWrapper(ValueUpdateHandler<U, T> handler, Class<T> dataType, Class<? extends UniqueData> holderClass) {
        if (handler.getClass().getDeclaredFields().length > 0) {
            throw new ValueUpdateHandlerNonStaticException("Value update handler must not capture any variables! It must act as a static function. Did you reference 'this' or a member variable? Use the provided instance instead!");
            // we don't want to hold a reference to a UniqueData instances, since it won't get GCed
            // and the handler may be called for any holder instance.
        }

        this.handler = handler;
        this.dataType = dataType;
        this.holderClass = holderClass;
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
