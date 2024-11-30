package net.staticstudios.data.mocks.netflix;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.DataProvider;

public class MockNetflixUserProvider extends DataProvider<MockNetflixUser> {
    public MockNetflixUserProvider(DataManager dataManager) {
        super(dataManager, MockNetflixUser.class);
    }

    public MockNetflixUser createUser() {
        MockNetflixUser user = new MockNetflixUser(getDataManager());
        set(user);

        return user;
    }
}
