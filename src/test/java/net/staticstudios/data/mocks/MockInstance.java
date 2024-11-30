package net.staticstudios.data.mocks;

import com.zaxxer.hikari.HikariConfig;
import net.staticstudios.data.DataManager;
import net.staticstudios.messaging.Messenger;

public abstract class MockInstance {
    private final DataManager dataManager;
    private final Messenger messenger;

    public MockInstance(String serverId, String redisHost, int redisPort, HikariConfig hikariConfig) {
        String primaryGroup = "mock";

        this.dataManager = new DataManager(
                serverId,
                hikariConfig,
                redisHost,
                redisPort
        );

        this.messenger = new Messenger(serverId, dataManager, primaryGroup);
        this.dataManager.setMessenger(this.messenger);
    }

    public DataManager getDataManager() {
        return dataManager;
    }

    public Messenger getMessenger() {
        return messenger;
    }
}