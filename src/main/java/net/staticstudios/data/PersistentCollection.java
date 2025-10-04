package net.staticstudios.data;

import com.google.common.base.Preconditions;
import net.staticstudios.data.util.Relation;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.AccessFlag;
import java.util.Collection;
import java.util.Iterator;

public interface PersistentCollection<T> extends Collection<T>, Relation<T> {

    static <T> PersistentCollection<T> of(UniqueData holder, Class<T> referenceType) {
        return new ProxyPersistentCollection<>(holder, referenceType);
    }

    //todo: add and remove handlers

    UniqueData getHolder();

    Class<T> getReferenceType();

    class ProxyPersistentCollection<T> implements PersistentCollection<T> {
        private final UniqueData holder;
        private final Class<T> referenceType;
        private @Nullable PersistentCollection<T> delegate;

        public ProxyPersistentCollection(UniqueData holder, Class<T> referenceType) {
            Preconditions.checkArgument(!holder.getClass().accessFlags().contains(AccessFlag.ABSTRACT), "Holder cannot be an abstract class! Please create this collection with the real class via PersistentCollection.of(...)");
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

        public void setDelegate(PersistentCollection<T> delegate) {
            Preconditions.checkState(this.delegate == null, "Delegate has already been set");
            this.delegate = delegate;
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
    }
}
