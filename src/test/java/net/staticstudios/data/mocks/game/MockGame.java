package net.staticstudios.data.mocks.game;

import com.zaxxer.hikari.HikariConfig;
import net.staticstudios.data.DataManager;
import net.staticstudios.messaging.Messenger;
import net.staticstudios.data.mocks.MockBlockLocationSerializer;
import net.staticstudios.data.mocks.MockOnlineStatusSerializer;

public abstract class MockGame<P extends MockGenericPlayer> {
    private final DataManager dataManager;
    private final Messenger messenger;

    public MockGame(String serverId, String redisHost, int redisPort, HikariConfig hikariConfig) {
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
    }

    abstract public MockPlayerProvider<P> getPlayerProvider();

    public DataManager getDataManager() {
        return dataManager;
    }
}
