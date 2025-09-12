package net.staticstudios.data.mock.persistentvalue;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.PersistentValue;
import net.staticstudios.data.UniqueData;

import java.util.UUID;

public class DiscordUserSettings extends UniqueData {
    private final PersistentValue<Boolean> enableFriendRequests = PersistentValue.of(this, Boolean.class, "enable_friend_requests")
            .withDefault(true);

    private DiscordUserSettings(DataManager dataManager, UUID id) {
        super(dataManager, "discord", "user_settings", "user_id", id);
    }

    public static DiscordUserSettings createSync(DataManager dataManager, String name) {
        DiscordUserSettings user = new DiscordUserSettings(dataManager, UUID.randomUUID());
        dataManager.insert(user);

        return user;
    }

    public boolean getEnableFriendRequests() {
        return enableFriendRequests.get();
    }

    public void setEnableFriendRequests(boolean enableFriendRequests) {
        this.enableFriendRequests.set(enableFriendRequests);
    }

}
