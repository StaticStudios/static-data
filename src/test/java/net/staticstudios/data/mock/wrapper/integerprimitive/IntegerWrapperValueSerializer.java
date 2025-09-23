package net.staticstudios.data.mock.wrapper.integerprimitive;

import net.staticstudios.data.ValueSerializer;
import org.jetbrains.annotations.NotNull;

public class IntegerWrapperValueSerializer implements ValueSerializer<IntegerWrapper, Integer> {
    @Override
    public IntegerWrapper deserialize(@NotNull Integer serialized) {
        return new IntegerWrapper(serialized);
    }

    @Override
    public Integer serialize(@NotNull IntegerWrapper deserialized) {
        return deserialized.value();
    }

    @Override
    public Class<IntegerWrapper> getDeserializedType() {
        return IntegerWrapper.class;
    }

    @Override
    public Class<Integer> getSerializedType() {
        return Integer.class;
    }
}

