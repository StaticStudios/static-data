package net.staticstudios.data.mocks.service;

import com.zaxxer.hikari.HikariConfig;
import net.staticstudios.data.DataManager;
import net.staticstudios.messaging.Messenger;
import net.staticstudios.data.mocks.MockBlockLocationSerializer;
import net.staticstudios.data.mocks.MockOnlineStatusSerializer;
import net.staticstudios.data.mocks.MockTestDataObject;
import net.staticstudios.data.mocks.MockTestDataObjectProvider;

public class MockService {
    private final DataManager dataManager;
    private final Messenger messenger;
    private final MockTestDataObjectProvider dataObjectProvider;

    public MockService(String serverId, String redisHost, int redisPort, HikariConfig hikariConfig) {
        String primaryGroup = "mock";

        this.dataManager = new DataManager(
                serverId,
                hikariConfig,
                redisHost,
                redisPort
        );

        this.messenger = new Messenger(serverId, dataManager, primaryGroup);
        this.dataManager.setMessenger(this.messenger);

        dataManager.registerSerializer(new MockBlockLocationSerializer());
        dataManager.registerSerializer(new MockOnlineStatusSerializer());

        this.dataObjectProvider = new MockTestDataObjectProvider(dataManager);
    }

    public MockTestDataObjectProvider getDataObjectProvider() {
        return dataObjectProvider;
    }

    public DataManager getDataManager() {
        return dataManager;
    }

    public MockTestDataObject createTestDataObject() {
        MockTestDataObject dataObject = new MockTestDataObject(dataManager);
        dataObjectProvider.set(dataObject);

        return dataObject;
    }
}
