package net.staticstudios.data.mock.wrapper.uuidprimitive;

import net.staticstudios.data.ValueSerializer;
import org.jetbrains.annotations.NotNull;

import java.util.UUID;

public class UUIDWrapperValueSerializer implements ValueSerializer<UUIDWrapper, UUID> {
    @Override
    public UUIDWrapper deserialize(@NotNull UUID serialized) {
        return new UUIDWrapper(serialized);
    }

    @Override
    public UUID serialize(@NotNull UUIDWrapper deserialized) {
        return deserialized.value();
    }

    @Override
    public Class<UUIDWrapper> getDeserializedType() {
        return UUIDWrapper.class;
    }

    @Override
    public Class<UUID> getSerializedType() {
        return UUID.class;
    }
}

