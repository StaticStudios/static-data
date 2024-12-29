package net.staticstudios.data;

public interface ValueSerializer<D, S> {
    D deserialize(S serialized);

    S serialize(D deserialized);

    Class<D> getDeserializedType();

    Class<S> getSerializedType();

    @SuppressWarnings("unchecked")
    default D unsafeDeserialize(Object serialized) {
        return deserialize((S) serialized);
    }

    @SuppressWarnings("unchecked")
    default S unsafeSerialize(Object deserialized) {
        return serialize((D) deserialized);
    }
}
