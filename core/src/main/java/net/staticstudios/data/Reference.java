package net.staticstudios.data;

import com.google.common.base.Preconditions;
import net.staticstudios.data.util.ReferenceMetadata;
import net.staticstudios.data.util.ReferenceUpdateHandler;
import net.staticstudios.data.util.ReferenceUpdateHandlerWrapper;
import net.staticstudios.data.util.Relation;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.AccessFlag;
import java.util.ArrayList;
import java.util.List;

public interface Reference<T extends UniqueData> extends Relation<T> {

    static <T extends UniqueData> Reference<T> of(UniqueData holder, Class<T> referenceType) {
        return new ProxyReference<>(holder, referenceType);
    }

    UniqueData getHolder();

    Class<T> getReferenceType();

    @Nullable T get();

    void set(@Nullable T value);

    <U extends UniqueData> Reference<T> onUpdate(Class<U> holderClass, ReferenceUpdateHandler<U, T> updateHandler);

    class ProxyReference<T extends UniqueData> implements Reference<T> {
        private final UniqueData holder;
        private final Class<T> referenceType;
        private final List<ReferenceUpdateHandlerWrapper<?, ?>> updateHandlers = new ArrayList<>();
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
        public <U extends UniqueData> Reference<T> onUpdate(Class<U> holderClass, ReferenceUpdateHandler<U, T> updateHandler) {
            Preconditions.checkArgument(delegate == null, "Cannot dynamically add an update handler after the holder has been initialized!");
            ReferenceUpdateHandlerWrapper<U, T> wrapper = new ReferenceUpdateHandlerWrapper<>(updateHandler);
            this.updateHandlers.add(wrapper);
            return this;
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

        public void setDelegate(ReferenceMetadata metadata, Reference<T> delegate) {
            Preconditions.checkState(this.delegate == null, "Delegate has already been set");
            this.delegate = delegate;
            holder.getDataManager().registerReferenceUpdateHandlers(metadata, updateHandlers);
        }
    }
}
