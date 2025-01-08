package net.staticstudios.data.mock.persistentvalue;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.PersistentValue;
import net.staticstudios.data.UniqueData;
import net.staticstudios.data.util.InsertionStrategy;

import java.util.UUID;

public class DiscordUser extends UniqueData {
    private final PersistentValue<Integer> nameUpdatesCalled = PersistentValue.foreign(this, Integer.class, "discord.user_meta.name_updates_called", "id")
            .withDefault(0);
    private final PersistentValue<Integer> enableFriendRequestsUpdatesCalled = PersistentValue.foreign(this, Integer.class, "discord.user_meta.enable_friend_requests_updates_called", "id")
            .withDefault(0);
    private final PersistentValue<String> name = PersistentValue.of(this, String.class, "name")
            .onUpdate(update -> {
                if (update.oldValue() == null) {
                    return;
                }
                nameUpdatesCalled.set(nameUpdatesCalled.get() + 1);
            });
    private final PersistentValue<Boolean> enableFriendRequests = PersistentValue.foreign(this, Boolean.class, "discord", "user_settings", "enable_friend_requests", "user_id")
            .withDefault(true)
            .insertionStrategy(InsertionStrategy.PREFER_EXISTING)
            .onUpdate(update -> {
                if (update.oldValue() == null) {
                    return;
                }
                enableFriendRequestsUpdatesCalled.set(enableFriendRequestsUpdatesCalled.get() + 1);
            });

    private DiscordUser(DataManager dataManager, UUID id) {
        super(dataManager, "discord", "users", id);
    }

    public static DiscordUser createSync(DataManager dataManager, String name, UUID id) {
        DiscordUser user = new DiscordUser(dataManager, id);
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

    public int getNameUpdatesCalled() {
        return nameUpdatesCalled.get();
    }

    public int getEnableFriendRequestsUpdatesCalled() {
        return enableFriendRequestsUpdatesCalled.get();
    }
}
