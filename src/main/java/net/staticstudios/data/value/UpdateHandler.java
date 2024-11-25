package net.staticstudios.data.value;

import net.staticstudios.data.UpdatedValue;

@FunctionalInterface
public interface UpdateHandler<T> {
    void onUpdate(UpdatedValue<T> updated);

    @SuppressWarnings("unchecked")
    default void unsafeHandleUpdate(Object originalValue, Object newValue) {
        onUpdate(new UpdatedValue<>((T) newValue, (T) originalValue));
    }
}
