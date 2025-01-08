package net.staticstudios.data.mock.reference;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.PersistentValue;
import net.staticstudios.data.Reference;
import net.staticstudios.data.UniqueData;

import java.util.UUID;

public class SnapchatUserSettings extends UniqueData {
    private final Reference<SnapchatUser> user = Reference.of(this, SnapchatUser.class, "user_id");
    private final PersistentValue<Boolean> enableFriendRequests = PersistentValue.of(this, Boolean.class, "enable_friend_requests")
            .withDefault(true);

    private SnapchatUserSettings(DataManager dataManager, UUID id) {
        super(dataManager, "snapchat", "user_settings", "user_id", id);
    }

    public static SnapchatUserSettings createSync(DataManager dataManager, UUID id) {
        SnapchatUserSettings settings = new SnapchatUserSettings(dataManager, id);
        dataManager.insert(settings);

        return settings;
    }

    public boolean getEnableFriendRequests() {
        return enableFriendRequests.get();
    }

    public void setEnableFriendRequests(boolean enableFriendRequests) {
        this.enableFriendRequests.set(enableFriendRequests);
    }

    public SnapchatUser getUser() {
        return user.get();
    }
}
