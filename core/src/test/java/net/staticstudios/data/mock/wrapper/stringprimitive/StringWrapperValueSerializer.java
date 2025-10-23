package net.staticstudios.data.mock.wrapper.stringprimitive;

import net.staticstudios.data.ValueSerializer;
import org.jetbrains.annotations.NotNull;

public class StringWrapperValueSerializer implements ValueSerializer<StringWrapper, String> {
    @Override
    public StringWrapper deserialize(@NotNull String serialized) {
        return new StringWrapper(serialized);
    }

    @Override
    public String serialize(@NotNull StringWrapper deserialized) {
        return deserialized.value();
    }

    @Override
    public Class<StringWrapper> getDeserializedType() {
        return StringWrapper.class;
    }

    @Override
    public Class<String> getSerializedType() {
        return String.class;
    }
}