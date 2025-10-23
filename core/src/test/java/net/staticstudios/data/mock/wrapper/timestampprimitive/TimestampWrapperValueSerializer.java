package net.staticstudios.data.mock.wrapper.timestampprimitive;

import net.staticstudios.data.ValueSerializer;
import org.jetbrains.annotations.NotNull;

import java.sql.Timestamp;

public class TimestampWrapperValueSerializer implements ValueSerializer<TimestampWrapper, Timestamp> {
    @Override
    public TimestampWrapper deserialize(@NotNull Timestamp serialized) {
        return new TimestampWrapper(serialized);
    }

    @Override
    public Timestamp serialize(@NotNull TimestampWrapper deserialized) {
        return deserialized.value();
    }

    @Override
    public Class<TimestampWrapper> getDeserializedType() {
        return TimestampWrapper.class;
    }

    @Override
    public Class<Timestamp> getSerializedType() {
        return Timestamp.class;
    }
}

