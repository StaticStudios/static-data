package net.staticstudios.data.mocks.game;

import com.zaxxer.hikari.HikariConfig;
import net.staticstudios.data.DataProvider;
import net.staticstudios.data.mocks.MockInstance;

public abstract class MockGame<P extends MockGenericPlayer> extends MockInstance {
    public MockGame(String serverId, String redisHost, int redisPort, HikariConfig hikariConfig) {
        super(serverId, redisHost, redisPort, hikariConfig);
    }

    abstract public DataProvider<P> getPlayerProvider();
}
