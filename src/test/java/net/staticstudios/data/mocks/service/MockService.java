package net.staticstudios.data.mocks.service;

import com.zaxxer.hikari.HikariConfig;
import net.staticstudios.data.mocks.MockInstance;
import net.staticstudios.data.mocks.MockTestDataObject;
import net.staticstudios.data.mocks.MockTestDataObjectProvider;

public class MockService extends MockInstance {
    private final MockTestDataObjectProvider dataObjectProvider;

    public MockService(String serverId, String redisHost, int redisPort, HikariConfig hikariConfig) {
        super(serverId, redisHost, redisPort, hikariConfig);

        this.dataObjectProvider = new MockTestDataObjectProvider(getDataManager());
    }

    public MockTestDataObjectProvider getDataObjectProvider() {
        return dataObjectProvider;
    }

    public MockTestDataObject createTestDataObject() {
        MockTestDataObject dataObject = new MockTestDataObject(getDataManager());
        dataObjectProvider.set(dataObject);

        return dataObject;
    }
}
