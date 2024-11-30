package net.staticstudios.data.value;

import net.staticstudios.data.UpdatedValue;

@FunctionalInterface
public interface UpdateHandler<T> {
    /**
     * Note that this is called after the value has been updated.
     *
     * @param updated The updated value.
     */
    void onUpdate(UpdatedValue<T> updated);

    @SuppressWarnings("unchecked")
    default void unsafeHandleUpdate(Object originalValue, Object newValue) {
        onUpdate((UpdatedValue<T>) new UpdatedValue<>(originalValue, newValue));
    }
}
