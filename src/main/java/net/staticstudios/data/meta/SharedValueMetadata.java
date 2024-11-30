package net.staticstudios.data.meta;

import net.staticstudios.data.UniqueData;
import net.staticstudios.data.shared.SharedValue;

import java.lang.reflect.Field;

public interface SharedValueMetadata<T extends SharedValue<?>> extends Metadata {

    /**
     * Gets the type of data stored in the shared value.
     *
     * @return the type of data stored in the shared value
     */
    Class<?> getType();

    /**
     * Gets the shared value from the instance.
     *
     * @param instance the instance to get the shared value from
     * @return the shared value from the instance
     */
    @SuppressWarnings("unchecked")
    default T getSharedValue(UniqueData instance) {
        Field field = getField();
        try {
            return (T) field.get(instance);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Failed to get value from field " + field.getName() + " in " + instance.getClass().getName(), e);
        }
    }

    /**
     * Sets the internal value of the shared value.
     *
     * @param instance the instance that contains the shared value
     * @param value    the value to set
     */
    default void setInternalValue(UniqueData instance, Object value) {
        SharedValue<?> sharedValue = getSharedValue(instance);
        sharedValue.setInternal(value);
    }

    /**
     * Get the table the parent object uses.
     *
     * @return The table the parent object uses
     */
    String getTable();
}
