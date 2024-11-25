package net.staticstudios.data.mocks.discord;

import com.zaxxer.hikari.HikariConfig;
import net.staticstudios.data.mocks.MockInstance;

public class MockDiscordBot extends MockInstance {
    private final MockDiscordUserProvider userProvider;
    private final MockDiscordUserStatsProvider userStatsProvider;


    public MockDiscordBot(String serverId, String redisHost, int redisPort, HikariConfig hikariConfig) {
        super(serverId, redisHost, redisPort, hikariConfig);

        userProvider = new MockDiscordUserProvider(getDataManager());
        userStatsProvider = new MockDiscordUserStatsProvider(getDataManager());
    }

    public MockDiscordUserProvider getUserProvider() {
        return userProvider;
    }

    public MockDiscordUserStatsProvider getUserStatsProvider() {
        return userStatsProvider;
    }
}
