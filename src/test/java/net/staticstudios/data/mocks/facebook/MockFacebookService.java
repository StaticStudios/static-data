package net.staticstudios.data.mocks.facebook;

import com.zaxxer.hikari.HikariConfig;
import net.staticstudios.data.DatabaseSupportedType;
import net.staticstudios.data.mocks.MockInstance;

/**
 * This service is used to test all {@link DatabaseSupportedType}s
 */
public class MockFacebookService extends MockInstance {
    private final MockFacebookUserProvider userProvider;

    public MockFacebookService(String serverId, String redisHost, int redisPort, HikariConfig hikariConfig) {
        super(serverId, redisHost, redisPort, hikariConfig);

        this.userProvider = new MockFacebookUserProvider(getDataManager());
    }

    public MockFacebookUserProvider getUserProvider() {
        return userProvider;
    }
}
