package net.staticstudios.data.shared;

import net.staticstudios.data.Addressable;
import net.staticstudios.data.meta.SharedValueMetadata;
import net.staticstudios.data.value.UpdateHandler;

/**
 * A class that represents a value which is shared and synced between multiple servers.
 */
public interface SharedValue<T> extends Addressable {
    /**
     * Get the current value
     *
     * @return the current value
     */
    T get();

    /**
     * Set the value.
     * This method will update the internal value, update the data source,
     * and will notify other servers via an async message.
     *
     * @param value the value to set
     */
    void set(T value);

    /**
     * Set the internal value only.
     * This will not update the data source or notify other servers.
     *
     * @param value the value to set
     */
    void setInternal(Object value);

    /**
     * Get the type of the value being stored.
     *
     * @return the type of the value
     */
    Class<T> getType();

    /**
     * Get the update handler for this value.
     *
     * @return the update handler
     */
    UpdateHandler<T> getUpdateHandler();


    /**
     * Get the metadata class for the collection
     *
     * @return The metadata class
     */
    public abstract Class<? extends SharedValueMetadata<?>> getMetadataClass();
}
