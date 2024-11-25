package net.staticstudios.data.mocks.game;

import com.zaxxer.hikari.HikariConfig;
import net.staticstudios.data.DataProvider;
import net.staticstudios.data.mocks.MockBlockLocationSerializer;
import net.staticstudios.data.mocks.MockInstance;
import net.staticstudios.data.mocks.MockOnlineStatusSerializer;

public abstract class MockGame<P extends MockGenericPlayer> extends MockInstance {
    public MockGame(String serverId, String redisHost, int redisPort, HikariConfig hikariConfig) {
        super(serverId, redisHost, redisPort, hikariConfig);

        getDataManager().registerSerializer(new MockBlockLocationSerializer());
        getDataManager().registerSerializer(new MockOnlineStatusSerializer());
    }

    abstract public DataProvider<P> getPlayerProvider();
}
