package net.staticstudios.data.mocks;

import net.staticstudios.data.ValueSerializer;

public class MockOnlineStatusSerializer implements ValueSerializer<MockOnlineStatus, Boolean> {
    @Override
    public Class<MockOnlineStatus> getDeserializedType() {
        return MockOnlineStatus.class;
    }

    @Override
    public Class<Boolean> getSerializedType() {
        return Boolean.class;
    }

    @Override
    public MockOnlineStatus deserialize(Object serialized) {
        return MockOnlineStatus.fromBoolean((Boolean) serialized);
    }

    @Override
    public Boolean serialize(Object deserialized) {
        return ((MockOnlineStatus) deserialized).isOnline();
    }
}
