package net.staticstudios.data.mocks.spotify;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.DataProvider;

public class MockSpotifyUserProvider extends DataProvider<MockSpotifyUser> {
    public MockSpotifyUserProvider(DataManager dataManager) {
        super(dataManager, MockSpotifyUser.class);
    }

    public MockSpotifyUser createUser(String name) {
        MockSpotifyUser user = new MockSpotifyUser(getDataManager(), name);
        set(user);
        return user;
    }
}
