package net.staticstudios.data;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import net.staticstudios.data.util.ColumnMetadata;
import net.staticstudios.data.util.PersistentValueMetadata;
import net.staticstudios.data.util.ValueUpdateHandler;
import net.staticstudios.data.util.ValueUpdateHandlerWrapper;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

//todo: keep this as an interface, since we'll allow the data accessor decide what to use. for example are we writing to the DB or the cache.

/**
 * A persistent value represents a single cell in a database table.
 *
 * @param <T>
 */
public interface PersistentValue<T> extends Value<T> {
    //todo: use caffeine to further cache pvs, provided we are using the H2 data accessor. allow us to toggle this on and off when setting up the data manager

    //todo: insert strategy, deletion strategy, update interval, update handling

    static <T> PersistentValue<T> of(UniqueData holder, Class<T> dataType) {
        return new ProxyPersistentValue<>(holder, dataType);
    }

    UniqueData getHolder();

    Class<T> getDataType();

    @ApiStatus.Internal
    Map<String, String> getIdColumnLinks();

    <U extends UniqueData> PersistentValue<T> onUpdate(Class<U> holderClass, ValueUpdateHandler<U, T> updateHandler);

    PersistentValue<T> withDefault(@Nullable T defaultValue);

    PersistentValue<T> withDefault(@Nullable Supplier<@Nullable T> defaultValueSupplier);


//    PersistentValue<T> updateInterval(long intervalMillis);

    class ProxyPersistentValue<T> implements PersistentValue<T> {
        protected final UniqueData holder;
        protected final Class<T> dataType;
        private final List<ValueUpdateHandlerWrapper<?, ?>> updateHandlers = new ArrayList<>();
        //todo: here's how update handlers should be stored
        // we store them globally on the data manager. we have a map of <schema.table.column, List<handler>>
        // additionally, for every field of type PV, we store the update handlers once - after the first unique data object is created. we will set them right before setting the delegate.
        // this also means that after weve set the delegate, we cannot add any more update handlers.
        // i think it would be useful to expose a method on the DM publicly to add an update handler to a specific column
        private @Nullable Supplier<@Nullable T> defaultValueSupplier;
        private @Nullable PersistentValue<T> delegate;
        private Map<String, String> idColumnLinks = Collections.emptyMap();
        private long updateIntervalMillis = -1;

        public ProxyPersistentValue(UniqueData holder, Class<T> dataType) {
            this.holder = holder;
            this.dataType = dataType;
        }

        public void setDelegate(ColumnMetadata columnMetadata, PersistentValue<T> delegate) {
            Preconditions.checkNotNull(delegate, "Delegate cannot be null");
            Preconditions.checkState(this.delegate == null, "Delegate is already set");
            this.delegate = delegate;
//            for (ValueUpdateHandler<T> handler : updateHandlers) {
//                this.delegate.onUpdate(handler);
//            }
//            this.updateHandlers.clear();
            if (this.defaultValueSupplier != null) {
                this.delegate.withDefault(this.defaultValueSupplier);
            }

            PersistentValueMetadata metadata = new PersistentValueMetadata(
                    holder.getClass(),
                    columnMetadata.schema(),
                    columnMetadata.table(),
                    columnMetadata.name(),
                    dataType
            );

            holder.getDataManager().registerUpdateHandler(metadata, updateHandlers);
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
        public Map<String, String> getIdColumnLinks() {
            return idColumnLinks;
        }
//
//        @Override
//        public PersistentValue<T> onUpdate(ValueUpdateHandler<T> updateHandler) {
//            Preconditions.checkNotNull(updateHandler, "Update handler cannot be null");
//
//            if (delegate != null) {
//                delegate.onUpdate(updateHandler);
//            } else {
//                this.updateHandlers.add(updateHandler);
//            }
//
//            return this;
//        }


        @Override
        public <U extends UniqueData> PersistentValue<T> onUpdate(Class<U> holderClass, ValueUpdateHandler<U, T> updateHandler) {
            Preconditions.checkArgument(delegate == null, "Cannot dynamically add an update handler after the holder has been initialized!");
            ValueUpdateHandlerWrapper<U, T> wrapper = new ValueUpdateHandlerWrapper<>(updateHandler, dataType, holderClass);
            this.updateHandlers.add(wrapper);
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

//        @Override
//        public PersistentValue<T> updateInterval(long intervalMillis) {
//            if (delegate != null) {
//                delegate.updateInterval(intervalMillis);
//                return this;
//            }
//            this.updateIntervalMillis = intervalMillis;
//            return this;
//        }

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
