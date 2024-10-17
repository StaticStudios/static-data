package net.staticstudios.data.mocks;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.DataProvider;
import net.staticstudios.data.UniqueData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class MockTestDataObjectProvider implements DataProvider<MockTestDataObject> {
    private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

    private final Map<UUID, MockTestDataObject> DATA = Collections.synchronizedMap(new HashMap<>());

    public MockTestDataObjectProvider(DataManager dataManager) {
        dataManager.registerDataProvider(this);
        loadAllSync(dataManager);
    }

    @Override
    public Class<MockTestDataObject> getDataType() {
        return MockTestDataObject.class;
    }

    @Override
    public Collection<MockTestDataObject> getAll() {
        return new HashSet<>(DATA.values());
    }

    @Override
    public MockTestDataObject get(UUID id) {
        return DATA.get(id);
    }

    @Override
    public void set(UniqueData data) {
        if (!(data instanceof MockTestDataObject object)) {
            throw new IllegalArgumentException("Data must be of type MockTestDataObject");
        }

        DATA.put(object.getId(), object);
    }

    @Override
    public void remove(UUID id) {
        DATA.remove(id);
    }

    @Override
    public Logger getLogger() {
        return LOGGER;
    }

    @Override
    public void clear() {
        DATA.clear();
    }
}
