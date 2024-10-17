package net.staticstudios.data.mocks.game.prison;

import com.zaxxer.hikari.HikariConfig;
import net.staticstudios.data.mocks.game.MockGame;
import net.staticstudios.data.mocks.game.MockPlayerProvider;

public class MockPrisonGame extends MockGame<MockPrisonPlayer> {
    private final MockPrisonPlayerProvider playerProvider;

    public MockPrisonGame(String serverId, String redisHost, int redisPort, HikariConfig hikariConfig) {
        super(serverId, redisHost, redisPort, hikariConfig);

        this.playerProvider = new MockPrisonPlayerProvider(getDataManager());
    }

    @Override
    public MockPlayerProvider<MockPrisonPlayer> getPlayerProvider() {
        return playerProvider;
    }
}
