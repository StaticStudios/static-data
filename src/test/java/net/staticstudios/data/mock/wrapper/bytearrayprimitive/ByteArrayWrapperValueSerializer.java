package net.staticstudios.data.mock.wrapper.bytearrayprimitive;

import net.staticstudios.data.ValueSerializer;
import org.jetbrains.annotations.NotNull;

public class ByteArrayWrapperValueSerializer implements ValueSerializer<ByteArrayWrapper, byte[]> {
    @Override
    public ByteArrayWrapper deserialize(@NotNull byte[] serialized) {
        return new ByteArrayWrapper(serialized);
    }

    @Override
    public byte[] serialize(@NotNull ByteArrayWrapper deserialized) {
        return deserialized.value();
    }

    @Override
    public Class<ByteArrayWrapper> getDeserializedType() {
        return ByteArrayWrapper.class;
    }

    @Override
    public Class<byte[]> getSerializedType() {
        return byte[].class;
    }
}

