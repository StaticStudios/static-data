package net.staticstudios.data;

import com.google.common.base.Preconditions;
import org.jetbrains.annotations.Nullable;

public interface Reference<T extends UniqueData> extends Relation<T> {

    UniqueData getHolder();

    Class<T> getReferenceType();

    @Nullable T get();

    void set(@Nullable T value);

    class ProxyReference<T extends UniqueData> implements Reference<T> {
        private final UniqueData holder;
        private final Class<T> referenceType;
        private @Nullable Reference<T> delegate;

        public ProxyReference(UniqueData holder, Class<T> referenceType) {
            this.holder = holder;
            this.referenceType = referenceType;
        }

        @Override
        public UniqueData getHolder() {
            return holder;
        }

        @Override
        public Class<T> getReferenceType() {
            return referenceType;
        }

        @Override
        public T get() {
            Preconditions.checkState(delegate != null, "Reference has not been initialized yet");
            return delegate.get();
        }

        @Override
        public void set(T value) {
            Preconditions.checkState(delegate != null, "Reference has not been initialized yet");
            delegate.set(value);
        }

        public void setDelegate(Reference<T> delegate) {
            Preconditions.checkState(this.delegate == null, "Delegate has already been set");
            this.delegate = delegate;
        }
    }
}
