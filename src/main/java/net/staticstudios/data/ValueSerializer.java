package net.staticstudios.data;

import java.lang.reflect.ParameterizedType;

public interface ValueSerializer<D, S> {
    D deserialize(S serialized);

    S serialize(D deserialized);

    @SuppressWarnings("unchecked")
    default Class<D> getDeserializedType() {
        return (Class<D>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
    }

    @SuppressWarnings("unchecked")
    default Class<S> getSerializedType() {
        return (Class<S>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[1];
    }

    default D unsafeDeserialize(Object serialized) {
        return deserialize((S) serialized);
    }

    default S unsafeSerialize(Object deserialized) {
        return serialize((D) deserialized);
    }
}
