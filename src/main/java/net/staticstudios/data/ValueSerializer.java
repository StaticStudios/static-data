package net.staticstudios.data;

/**
 * A serializer for non-primitive types.
 * See {@link net.staticstudios.data.primative.Primitives} for primitive types.
 * Nullability depends on the implementation.
 *
 * @param <D> The deserialized type
 * @param <S> The serialized type
 */
public interface ValueSerializer<D, S> {
    /**
     * Deserialize the serialized object
     *
     * @param serialized The serialized object
     * @return The deserialized object
     */
    D deserialize(S serialized);

    /**
     * Serialize the deserialized object
     *
     * @param deserialized The deserialized object
     * @return The serialized object
     */
    S serialize(D deserialized);

    /**
     * Get the deserialized type
     *
     * @return The deserialized type
     */
    Class<D> getDeserializedType();

    /**
     * Get the serialized type
     *
     * @return The serialized type
     */
    Class<S> getSerializedType();
}
