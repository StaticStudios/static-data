package net.staticstudios.data.mock.wrapper.longprimitive;

import net.staticstudios.data.ValueSerializer;
import org.jetbrains.annotations.NotNull;

public class LongWrapperValueSerializer implements ValueSerializer<LongWrapper, Long> {
    @Override
    public LongWrapper deserialize(@NotNull Long serialized) {
        return new LongWrapper(serialized);
    }

    @Override
    public Long serialize(@NotNull LongWrapper deserialized) {
        return deserialized.value();
    }

    @Override
    public Class<LongWrapper> getDeserializedType() {
        return LongWrapper.class;
    }

    @Override
    public Class<Long> getSerializedType() {
        return Long.class;
    }
}

