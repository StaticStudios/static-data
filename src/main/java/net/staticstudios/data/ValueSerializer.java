package net.staticstudios.data;

public interface ValueSerializer<D, S> {

    /**
     * The type to be used by the application.
     *
     * @return The type to be used by the application.
     */
    Class<D> getDeserializedType();

    /**
     * The type to be used for storage.
     *
     * @return The type to be used for storage.
     */
    Class<S> getSerializedType();

    /**
     * Convert a serialized object to a deserialized object.
     *
     * @param serialized The serialized object.
     * @return The deserialized object.
     */
    D deserialize(Object serialized);

    /**
     * Convert a deserialized object to a serialized object.
     *
     * @param deserialized The deserialized object.
     * @return The serialized object.
     */
    S serialize(Object deserialized);
}
