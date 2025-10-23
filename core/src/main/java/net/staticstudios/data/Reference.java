package net.staticstudios.data;

import com.google.common.base.Preconditions;
import net.staticstudios.data.util.Relation;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.AccessFlag;

public interface Reference<T extends UniqueData> extends Relation<T> {

    static <T extends UniqueData> Reference<T> of(UniqueData holder, Class<T> referenceType) {
        return new ProxyReference<>(holder, referenceType);
    }

    UniqueData getHolder();

    Class<T> getReferenceType();

    @Nullable T get();

    void set(@Nullable T value);

    class ProxyReference<T extends UniqueData> implements Reference<T> {
        private final UniqueData holder;
        private final Class<T> referenceType;
        private @Nullable Reference<T> delegate;

        public ProxyReference(UniqueData holder, Class<T> referenceType) {
            Preconditions.checkArgument(!holder.getClass().accessFlags().contains(AccessFlag.ABSTRACT), "Holder cannot be an abstract class! Please create this reference with the real class via Reference.of(...)");
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
