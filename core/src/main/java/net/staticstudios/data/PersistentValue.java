package net.staticstudios.data;

import com.google.common.base.Preconditions;
import net.staticstudios.data.util.*;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * A persistent value represents a single cell in a database referringTable.
 *
 * @param <T>
 */
public interface PersistentValue<T> extends Value<T> {

    static <T> PersistentValue<T> of(UniqueData holder, Class<T> dataType) {
        return new ProxyPersistentValue<>(holder, dataType);
    }

    UniqueData getHolder();

    Class<T> getDataType();

    <U extends UniqueData> PersistentValue<T> onUpdate(Class<U> holderClass, ValueUpdateHandler<U, T> updateHandler);

    class ProxyPersistentValue<T> implements PersistentValue<T> {
        protected final UniqueData holder;
        protected final Class<T> dataType;
        private final List<ValueUpdateHandlerWrapper<?, ?>> updateHandlers = new ArrayList<>();
        private @Nullable PersistentValue<T> delegate;

        public ProxyPersistentValue(UniqueData holder, Class<T> dataType) {
            this.holder = holder;
            this.dataType = dataType;
        }

        public void setDelegate(PersistentValueMetadata metadata, PersistentValue<T> delegate) {
            Preconditions.checkNotNull(delegate, "Delegate cannot be null");
            Preconditions.checkState(this.delegate == null, "Delegate is already set");

            if (!metadata.hasValidatedUpdateHandlers()) {
                for (ValueUpdateHandlerWrapper<?, ?> wrapper : updateHandlers) {
                    LambdaUtils.assertLambdaDoesntCapture(wrapper.getHandler(), List.of(UniqueData.class), null);
                }
                metadata.setValidatedUpdateHandlers(true);
            }

            this.delegate = delegate;
            holder.getDataManager().registerPersistentValueUpdateHandlers(metadata, updateHandlers);
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
        public <U extends UniqueData> PersistentValue<T> onUpdate(Class<U> holderClass, ValueUpdateHandler<U, T> updateHandler) {
            Preconditions.checkArgument(delegate == null, "Cannot dynamically add an update handler after the holder has been initialized!");
            ValueUpdateHandlerWrapper<U, T> wrapper = new ValueUpdateHandlerWrapper<>(updateHandler, dataType, holderClass);
            this.updateHandlers.add(wrapper);
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
        public void set(T value) {
            if (delegate != null) {
                delegate.set(value);
                return;
            }
            throw new UnsupportedOperationException("Not implemented");
        }
    }
}
