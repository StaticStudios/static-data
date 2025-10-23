package net.staticstudios.data.mock.wrapper.booleanprimitive;

import net.staticstudios.data.ValueSerializer;
import org.jetbrains.annotations.NotNull;

public class BooleanWrapperValueSerializer implements ValueSerializer<BooleanWrapper, Boolean> {
    @Override
    public BooleanWrapper deserialize(@NotNull Boolean serialized) {
        return new BooleanWrapper(serialized);
    }

    @Override
    public Boolean serialize(@NotNull BooleanWrapper deserialized) {
        return deserialized.value();
    }

    @Override
    public Class<BooleanWrapper> getDeserializedType() {
        return BooleanWrapper.class;
    }

    @Override
    public Class<Boolean> getSerializedType() {
        return Boolean.class;
    }
}

