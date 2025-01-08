package net.staticstudios.data.data.value;

import net.staticstudios.data.data.Data;
import org.jetbrains.annotations.Nullable;

public interface Value<T> extends Data<T> {
    /**
     * Get the value of this object
     *
     * @return the value, or null if the value is not set
     */
    T get();

    /**
     * Set the value of this object.
     * Depending on the type of value, null may be a valid value.
     * See {@link net.staticstudios.data.primative.Primitives} for more information.
     *
     * @param value the value to set
     */
    void set(@Nullable T value);
}
