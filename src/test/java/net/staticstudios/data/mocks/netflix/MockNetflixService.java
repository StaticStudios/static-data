package net.staticstudios.data.mocks.netflix;

import com.zaxxer.hikari.HikariConfig;
import net.staticstudios.data.DatabaseSupportedType;
import net.staticstudios.data.mocks.MockInstance;

/**
 * This service is used to test all {@link DatabaseSupportedType}s
 */
public class MockNetflixService extends MockInstance {
    private final MockNetflixUserProvider userProvider;

    public MockNetflixService(String serverId, String redisHost, int redisPort, HikariConfig hikariConfig) {
        super(serverId, redisHost, redisPort, hikariConfig);

        this.userProvider = new MockNetflixUserProvider(getDataManager());
    }

    public MockNetflixUserProvider getUserProvider() {
        return userProvider;
    }
}
