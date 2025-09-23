package net.staticstudios.data.mock.wrapper.doubleprimitive;

import net.staticstudios.data.ValueSerializer;
import org.jetbrains.annotations.NotNull;

public class DoubleWrapperValueSerializer implements ValueSerializer<DoubleWrapper, Double> {
    @Override
    public DoubleWrapper deserialize(@NotNull Double serialized) {
        return new DoubleWrapper(serialized);
    }

    @Override
    public Double serialize(@NotNull DoubleWrapper deserialized) {
        return deserialized.value();
    }

    @Override
    public Class<DoubleWrapper> getDeserializedType() {
        return DoubleWrapper.class;
    }

    @Override
    public Class<Double> getSerializedType() {
        return Double.class;
    }
}

