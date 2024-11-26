package net.staticstudios.data.mocks.reddit;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.DataProvider;

public class MockRedditUserProvider extends DataProvider<MockRedditUser> {
    public MockRedditUserProvider(DataManager dataManager) {
        super(dataManager, MockRedditUser.class);
    }

    public MockRedditUser createUser() {
        MockRedditUser user = new MockRedditUser(getDataManager());
        set(user);

        return user;
    }
}
