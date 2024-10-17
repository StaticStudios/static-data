package net.staticstudios.data.mocks.game.skyblock;

import com.zaxxer.hikari.HikariConfig;
import net.staticstudios.data.mocks.game.MockGame;
import net.staticstudios.data.mocks.game.MockPlayerProvider;

public class MockSkyblockGame extends MockGame<MockSkyblockPlayer> {
    private final MockSkyblockPlayerProvider playerProvider;

    public MockSkyblockGame(String serverId, String redisHost, int redisPort, HikariConfig hikariConfig) {
        super(serverId, redisHost, redisPort, hikariConfig);

        this.playerProvider = new MockSkyblockPlayerProvider(getDataManager());
    }

    @Override
    public MockPlayerProvider<MockSkyblockPlayer> getPlayerProvider() {
        return playerProvider;
    }
}
