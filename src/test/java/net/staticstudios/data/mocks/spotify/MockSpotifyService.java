package net.staticstudios.data.mocks.spotify;

import com.zaxxer.hikari.HikariConfig;
import net.staticstudios.data.mocks.MockInstance;
import net.staticstudios.data.value.PersistentValue;

/**
 * This service is used to test {@link PersistentValue}s
 */
public class MockSpotifyService extends MockInstance {
    private final MockSpotifyUserProvider userProvider;


    public MockSpotifyService(String serverId, String redisHost, int redisPort, HikariConfig hikariConfig) {
        super(serverId, redisHost, redisPort, hikariConfig);

        userProvider = new MockSpotifyUserProvider(getDataManager());
    }

    public MockSpotifyUserProvider getUserProvider() {
        return userProvider;
    }

}
