package net.staticstudios.data.value;

@FunctionalInterface
public interface UpdateHandler<T> {
    T onUpdate(T originalValue, T newValue);

    default T unsafeHandleUpdate(Object originalValue, Object newValue) {
        return onUpdate((T) originalValue, (T) newValue);
    }
}
