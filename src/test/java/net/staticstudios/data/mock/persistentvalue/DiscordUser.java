package net.staticstudios.data.mock.persistentvalue;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.data.PersistentValue;
import net.staticstudios.data.data.UniqueData;

import java.util.UUID;

public class DiscordUser extends UniqueData {
    private final PersistentValue<String> name = PersistentValue.of(this, String.class, "name");
    private final PersistentValue<Boolean> enableFriendRequests = PersistentValue.foreign(this, Boolean.class, "discord", "user_settings", "enable_friend_requests", "user_id")
            .withDefault(true)
            .onUpdate(update -> {
                //todo: test this update handler
                //todo: also ensure with dummy PVs this doesnt break anything.
            });

    private DiscordUser(DataManager dataManager, UUID id) {
        super(dataManager, "discord", "users", id);
    }

    public static DiscordUser createSync(DataManager dataManager, String name) {
        DiscordUser user = new DiscordUser(dataManager, UUID.randomUUID());
        dataManager.insert(user, user.name.initial(name));

        return user;
    }

    public String getName() {
        return name.get();
    }

    public void setName(String name) {
        this.name.set(name);
    }

    public boolean getEnableFriendRequests() {
        return enableFriendRequests.get();
    }

    public void setEnableFriendRequests(boolean enableFriendRequests) {
        this.enableFriendRequests.set(enableFriendRequests);
    }
}
