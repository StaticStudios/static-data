package net.staticstudios.data.mock.wrapper.floatprimitive;

import net.staticstudios.data.ValueSerializer;
import org.jetbrains.annotations.NotNull;

public class FloatWrapperValueSerializer implements ValueSerializer<FloatWrapper, Float> {
    @Override
    public FloatWrapper deserialize(@NotNull Float serialized) {
        return new FloatWrapper(serialized);
    }

    @Override
    public Float serialize(@NotNull FloatWrapper deserialized) {
        return deserialized.value();
    }

    @Override
    public Class<FloatWrapper> getDeserializedType() {
        return FloatWrapper.class;
    }

    @Override
    public Class<Float> getSerializedType() {
        return Float.class;
    }
}

