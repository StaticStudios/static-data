package net.staticstudios.data;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import net.staticstudios.data.util.ValueUpdateHandler;
import org.jetbrains.annotations.Nullable;

import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;

//todo: keep this as an interface, since we'll allow the data accessor decide what to use. for example are we writing to the real db or the cache.

/**
 * A persistent value represents a single cell in a database table.
 *
 * @param <T>
 */
public interface PersistentValue<T> {

    static <T> PersistentValue<T> of(UniqueData holder, Class<T> dataType) {
        return new ProxyPersistentValue<>(holder, dataType);
    }

    String getSchema();

    String getTable();

    String getColumn();

    PersistentValue<T> onUpdate(ValueUpdateHandler<T> updateHandler);

    PersistentValue<T> withDefault(@Nullable T defaultValue);

    PersistentValue<T> withDefault(@Nullable Supplier<@Nullable T> defaultValueSupplier);

    T get();

    void set(T value);

    class ProxyPersistentValue<T> implements PersistentValue<T> {
        protected final UniqueData holder;
        protected final Class<T> dataType;
        private final Deque<ValueUpdateHandler<T>> updateHandlers = new ConcurrentLinkedDeque<>();
        private @Nullable Supplier<@Nullable T> defaultValueSupplier;
        private @Nullable PersistentValue<T> delegate;


        public ProxyPersistentValue(UniqueData holder, Class<T> dataType) {
            this.holder = holder;
            this.dataType = dataType;
        }

        public void setDelegate(PersistentValue<?> delegate) {
            Preconditions.checkNotNull(delegate, "Delegate cannot be null");
            Preconditions.checkState(this.delegate == null, "Delegate is already set");
            this.delegate = (PersistentValue<T>) delegate;
            for (ValueUpdateHandler<T> handler : updateHandlers) {
                this.delegate.onUpdate(handler);
            }
            this.updateHandlers.clear();
            if (this.defaultValueSupplier != null) {
                this.delegate.withDefault(this.defaultValueSupplier);
            }
        }

        @Override
        public String getSchema() {
            Preconditions.checkState(delegate != null, "Delegate is not set");
            return delegate.getSchema();
        }

        @Override
        public String getTable() {
            Preconditions.checkState(delegate != null, "Delegate is not set");
            return delegate.getTable();
        }

        @Override
        public String getColumn() {
            Preconditions.checkState(delegate != null, "Delegate is not set");
            return delegate.getColumn();
        }

        @Override
        public PersistentValue<T> onUpdate(ValueUpdateHandler<T> updateHandler) {
            Preconditions.checkNotNull(updateHandler, "Update handler cannot be null");

            if (delegate != null) {
                delegate.onUpdate(updateHandler);
            } else {
                this.updateHandlers.add(updateHandler);
            }

            return this;
        }

        @Override
        public PersistentValue<T> withDefault(@Nullable Supplier<@Nullable T> defaultValueSupplier) {
            if (delegate != null) {
                delegate.withDefault(defaultValueSupplier);
            } else {
                this.defaultValueSupplier = defaultValueSupplier;
            }
            return this;
        }

        @Override
        public PersistentValue<T> withDefault(@Nullable T defaultValue) {
            return withDefault(() -> defaultValue);
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
