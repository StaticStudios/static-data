package net.staticstudios.data.mocks.reddit;

import com.zaxxer.hikari.HikariConfig;
import net.staticstudios.data.DatabaseSupportedType;
import net.staticstudios.data.mocks.MockInstance;

/**
 * This service is used to test all {@link DatabaseSupportedType}s
 */
public class MockRedditService extends MockInstance {
    private final MockRedditUserProvider userProvider;

    public MockRedditService(String serverId, String redisHost, int redisPort, HikariConfig hikariConfig) {
        super(serverId, redisHost, redisPort, hikariConfig);

        this.userProvider = new MockRedditUserProvider(getDataManager());
    }

    public MockRedditUserProvider getUserProvider() {
        return userProvider;
    }
}
