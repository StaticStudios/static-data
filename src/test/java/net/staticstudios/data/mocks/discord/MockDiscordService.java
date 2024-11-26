package net.staticstudios.data.mocks.discord;

import com.zaxxer.hikari.HikariConfig;
import net.staticstudios.data.mocks.MockInstance;
import net.staticstudios.data.value.ForeignPersistentValue;

/**
 * This service is used to test {@link ForeignPersistentValue}s
 */
public class MockDiscordService extends MockInstance {
    private final MockDiscordUserProvider userProvider;
    private final MockDiscordUserStatsProvider userStatsProvider;


    public MockDiscordService(String serverId, String redisHost, int redisPort, HikariConfig hikariConfig) {
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
