package net.staticstudios.data.util;

import net.staticstudios.data.UniqueData;

import java.util.Objects;

public class ValueUpdateHandlerWrapper<U extends UniqueData, T> {
    private final ValueUpdateHandler<U, T> handler;
    private final Class<T> dataType;
    private final Class<? extends UniqueData> holderClass;

    public ValueUpdateHandlerWrapper(ValueUpdateHandler<U, T> handler, Class<T> dataType, Class<? extends UniqueData> holderClass) {
        LambdaUtils.assertLambdaDoesntCapture(handler, "Use thr provided instance to access member variables.");
        // we don't want to hold a reference to a UniqueData instances, since it won't get GCed
        // and the handler may be called for any holder instance.

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
