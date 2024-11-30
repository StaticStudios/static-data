package net.staticstudios.data.mocks.facebook;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.DataProvider;

import java.util.Collection;

public class MockFacebookUserProvider extends DataProvider<MockFacebookUser> {
    public MockFacebookUserProvider(DataManager dataManager) {
        super(dataManager, MockFacebookUser.class);
    }

    public MockFacebookUser createUser() {
        MockFacebookUser user = new MockFacebookUser(getDataManager());
        set(user);

        return user;
    }

    public MockFacebookUser createUser(Collection<MockFacebookFriendEntry> friends) {
        MockFacebookUser user = new MockFacebookUser(getDataManager(), friends);
        set(user);

        return user;
    }
}
