package net.staticstudios.data;

import com.google.common.base.Preconditions;
import net.staticstudios.data.impl.data.AbstractCachedValue;
import net.staticstudios.data.util.*;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * A cached value represents a piece of data in redis.
 *
 * @param <T>
 */
public interface CachedValue<T> extends Value<T> {

    static <T> CachedValue<T> of(UniqueData holder, Class<T> dataType) {
        return new ProxyCachedValue<>(holder, dataType);
    }

    UniqueData getHolder();

    Class<T> getDataType();

    <U extends UniqueData> CachedValue<T> onUpdate(Class<U> holderClass, ValueUpdateHandler<U, T> updateHandler);

    default CachedValue<T> withFallback(T fallback) {
        return supplyFallback(() -> fallback);
    }

    <U extends UniqueData> CachedValue<T> refresher(Class<U> clazz, CachedValueRefresher<U, T> refresher);

    CachedValue<T> supplyFallback(Supplier<T> fallback);

    @Nullable T refresh();

    class ProxyCachedValue<T> implements CachedValue<T> {
        protected final UniqueData holder;
        protected final Class<T> dataType;
        private final List<ValueUpdateHandlerWrapper<?, T>> updateHandlers = new ArrayList<>();
        private @Nullable CachedValue<T> delegate;
        private Supplier<T> fallback = () -> null;
        private @Nullable CachedValueRefresher<UniqueData, T> refresher;

        public ProxyCachedValue(UniqueData holder, Class<T> dataType) {
            this.holder = holder;
            this.dataType = dataType;
        }

        public void setDelegate(CachedValueMetadata metadata, AbstractCachedValue<T> delegate) {
            Preconditions.checkNotNull(delegate, "Delegate cannot be null");
            Preconditions.checkState(this.delegate == null, "Delegate is already set");
            delegate.setFallback(this.fallback);
            delegate.setRefresher(refresher);
            this.delegate = delegate;

            //since an update handler can be registered before the fallback is set, we need to convert them here
            List<CachedValueUpdateHandlerWrapper<?, ?>> cachedValueUpdateHandlers = new ArrayList<>();
            for (ValueUpdateHandlerWrapper<?, T> handler : updateHandlers) {
                cachedValueUpdateHandlers.add(asCachedValueHandler(handler));
            }

            holder.getDataManager().registerCachedValueUpdateHandlers(metadata, cachedValueUpdateHandlers);
        }

        @Override
        public UniqueData getHolder() {
            return holder;
        }

        @Override
        public Class<T> getDataType() {
            return dataType;
        }

        @Override
        public <U extends UniqueData> CachedValue<T> onUpdate(Class<U> holderClass, ValueUpdateHandler<U, T> updateHandler) {
            Preconditions.checkArgument(delegate == null, "Cannot dynamically add an update handler after the holder has been initialized!");
            ValueUpdateHandlerWrapper<U, T> wrapper = new ValueUpdateHandlerWrapper<>(updateHandler, dataType, holderClass);
            this.updateHandlers.add(wrapper);
            return this;
        }

        @Override
        public CachedValue<T> supplyFallback(Supplier<T> fallback) {
            if (delegate != null) {
                throw new UnsupportedOperationException("Cannot set fallback after initialization");
            }
            Preconditions.checkNotNull(fallback, "Fallback supplier cannot be null");
            LambdaUtils.assertLambdaDoesntCapture(fallback, List.of(UniqueData.class), null);
            this.fallback = fallback;
            return this;
        }

        @SuppressWarnings("unchecked")
        @Override
        public <U extends UniqueData> CachedValue<T> refresher(Class<U> clazz, CachedValueRefresher<U, T> refresher) {
            Preconditions.checkArgument(delegate == null, "Cannot dynamically add a refresher after the holder has been initialized!");
            LambdaUtils.assertLambdaDoesntCapture(refresher, List.of(UniqueData.class), null);
            this.refresher = (CachedValueRefresher<UniqueData, T>) refresher;
            return this;
        }

        @Override
        public T get() {
            if (delegate != null) {
                return delegate.get();
            }
            throw new UnsupportedOperationException("Not implemented");
        }

        @Override
        public void set(@Nullable T value) {
            if (delegate != null) {
                delegate.set(value);
                return;
            }
            throw new UnsupportedOperationException("Not implemented");
        }

        @Override
        public @Nullable T refresh() {
            if (delegate != null) {
                return delegate.refresh();
            }
            throw new UnsupportedOperationException("Not implemented");
        }

        private <U extends UniqueData> CachedValueUpdateHandlerWrapper<U, T> asCachedValueHandler(ValueUpdateHandlerWrapper<U, T> handlerWrapper) {
            return new CachedValueUpdateHandlerWrapper<>(
                    handlerWrapper.getHandler(),
                    handlerWrapper.getDataType(),
                    handlerWrapper.getHolderClass(),
                    this.fallback
            );
        }
    }
}
