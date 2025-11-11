package net.staticstudios.data;

import com.google.common.base.Preconditions;
import net.staticstudios.data.util.CollectionChangeHandler;
import net.staticstudios.data.util.CollectionChangeHandlerWrapper;
import net.staticstudios.data.util.PersistentCollectionMetadata;
import net.staticstudios.data.util.Relation;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.AccessFlag;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public interface PersistentCollection<T> extends Collection<T>, Relation<T> {

    static <T> PersistentCollection<T> of(UniqueData holder, Class<T> referenceType) {
        return new ProxyPersistentCollection<>(holder, referenceType);
    }

    <U extends UniqueData> PersistentCollection<T> onAdd(Class<U> holderClass, CollectionChangeHandler<U, T> addHandler);

    <U extends UniqueData> PersistentCollection<T> onRemove(Class<U> holderClass, CollectionChangeHandler<U, T> removeHandler);

    UniqueData getHolder();

    class ProxyPersistentCollection<T> implements PersistentCollection<T> {
        private final UniqueData holder;
        private final Class<T> referenceType;
        private final List<CollectionChangeHandlerWrapper<?, ?>> changeHandlers = new ArrayList<>();
        private @Nullable PersistentCollection<T> delegate;

        public ProxyPersistentCollection(UniqueData holder, Class<T> referenceType) {
            Preconditions.checkArgument(!holder.getClass().accessFlags().contains(AccessFlag.ABSTRACT), "Holder cannot be an abstract class! Please create this collection with the real class via PersistentCollection.of(...)");
            this.holder = holder;
            this.referenceType = referenceType;
        }

        @Override
        public <U extends UniqueData> PersistentCollection<T> onAdd(Class<U> holderClass, CollectionChangeHandler<U, T> addHandler) {
            changeHandlers.add(new CollectionChangeHandlerWrapper<>(addHandler, referenceType, holder.getClass(), CollectionChangeHandlerWrapper.Type.ADD));
            return this;
        }

        @Override
        public <U extends UniqueData> PersistentCollection<T> onRemove(Class<U> holderClass, CollectionChangeHandler<U, T> removeHandler) {
            changeHandlers.add(new CollectionChangeHandlerWrapper<>(removeHandler, referenceType, holder.getClass(), CollectionChangeHandlerWrapper.Type.REMOVE));
            return this;
        }

        @Override
        public UniqueData getHolder() {
            return holder;
        }

        public Class<T> getDataType() {
            return referenceType;
        }

        public void setDelegate(PersistentCollectionMetadata metadata, PersistentCollection<T> delegate) {
            Preconditions.checkState(this.delegate == null, "Delegate has already been set");
            this.delegate = delegate;
            holder.getDataManager().registerCollectionChangeHandlers(metadata, changeHandlers);
        }

        @Override
        public int size() {
            Preconditions.checkState(delegate != null, "PersistentCollection has not been initialized yet");
            return delegate.size();
        }

        @Override
        public boolean isEmpty() {
            Preconditions.checkState(delegate != null, "PersistentCollection has not been initialized yet");
            return delegate.isEmpty();
        }

        @Override
        public boolean contains(Object o) {
            Preconditions.checkState(delegate != null, "PersistentCollection has not been initialized yet");
            return delegate.contains(o);
        }

        @Override
        public @NotNull Iterator<T> iterator() {
            Preconditions.checkState(delegate != null, "PersistentCollection has not been initialized yet");
            return delegate.iterator();
        }

        @Override
        public @NotNull Object @NotNull [] toArray() {
            Preconditions.checkState(delegate != null, "PersistentCollection has not been initialized yet");
            return delegate.toArray();
        }

        @Override
        public @NotNull <T1> T1 @NotNull [] toArray(@NotNull T1 @NotNull [] a) {
            Preconditions.checkState(delegate != null, "PersistentCollection has not been initialized yet");
            return delegate.toArray(a);
        }

        @Override
        public boolean add(T t) {
            Preconditions.checkState(delegate != null, "PersistentCollection has not been initialized yet");
            return delegate.add(t);
        }

        @Override
        public boolean remove(Object o) {
            Preconditions.checkState(delegate != null, "PersistentCollection has not been initialized yet");
            return delegate.remove(o);
        }

        @Override
        public boolean containsAll(@NotNull Collection<?> c) {
            Preconditions.checkState(delegate != null, "PersistentCollection has not been initialized yet");
            return delegate.containsAll(c);
        }

        @Override
        public boolean addAll(@NotNull Collection<? extends T> c) {
            Preconditions.checkState(delegate != null, "PersistentCollection has not been initialized yet");
            return delegate.addAll(c);
        }

        @Override
        public boolean removeAll(@NotNull Collection<?> c) {
            Preconditions.checkState(delegate != null, "PersistentCollection has not been initialized yet");
            return delegate.removeAll(c);
        }

        @Override
        public boolean retainAll(@NotNull Collection<?> c) {
            Preconditions.checkState(delegate != null, "PersistentCollection has not been initialized yet");
            return delegate.retainAll(c);
        }

        @Override
        public void clear() {
            Preconditions.checkState(delegate != null, "PersistentCollection has not been initialized yet");
            delegate.clear();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            PersistentCollection<?> delegate = null;
            if (obj instanceof PersistentCollection.ProxyPersistentCollection<?> proxyPersistentCollection) {
                delegate = proxyPersistentCollection.delegate;
            } else if (obj instanceof PersistentCollection<?> persistentCollection) {
                delegate = persistentCollection;
            }

            Preconditions.checkState(this.delegate != null, "PersistentCollection has not been initialized yet");

            return this.delegate.equals(delegate);
        }

        @Override
        public int hashCode() {
            Preconditions.checkState(delegate != null, "PersistentCollection has not been initialized yet");
            return delegate.hashCode();
        }

        @Override
        public String toString() {
            Preconditions.checkState(delegate != null, "PersistentCollection has not been initialized yet");
            return delegate.toString();
        }
    }
}
