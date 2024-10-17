package net.staticstudios.data.mocks;

import net.staticstudios.data.ValueSerializer;

public class MockBlockLocationSerializer implements ValueSerializer<MockBlockLocation, String> {
    @Override
    public Class<MockBlockLocation> getDeserializedType() {
        return MockBlockLocation.class;
    }

    @Override
    public Class<String> getSerializedType() {
        return String.class;
    }

    @Override
    public MockBlockLocation deserialize(Object serialized) {
        String string = (String) serialized;
        String[] parts = string.split(",", 3);
        return new MockBlockLocation(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]), Integer.parseInt(parts[2]));
    }

    @Override
    public String serialize(Object deserialized) {
        MockBlockLocation location = (MockBlockLocation) deserialized;
        return "%s,%s,%s".formatted(
                location.x(),
                location.y(),
                location.z()
        );
    }
}
