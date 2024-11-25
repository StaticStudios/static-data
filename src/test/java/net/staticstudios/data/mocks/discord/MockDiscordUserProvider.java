package net.staticstudios.data.mocks.discord;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.DataProvider;

import java.util.UUID;

public class MockDiscordUserProvider extends DataProvider<MockDiscordUser> {
    public MockDiscordUserProvider(DataManager dataManager) {
        super(dataManager, MockDiscordUser.class);
    }

    public MockDiscordUser createUser(String name) {
        MockDiscordUser user = new MockDiscordUser(getDataManager(), UUID.randomUUID(), name);
        set(user);
        return user;
    }
}
