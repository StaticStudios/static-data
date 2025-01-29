package net.staticstudios.data.mock.reference;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.PersistentValue;
import net.staticstudios.data.Reference;
import net.staticstudios.data.UniqueData;
import org.jetbrains.annotations.Nullable;

import java.util.UUID;

public class SnapchatUser extends UniqueData {
    private final PersistentValue<Integer> updateCalled = PersistentValue.foreign(this, Integer.class, "snapchat.user_meta.update_called", "id")
            .withDefault(0);
    private final Reference<SnapchatUserSettings> settings = Reference.of(this, SnapchatUserSettings.class, "id");
    private final Reference<SnapchatUser> favoriteUser = Reference.of(this, SnapchatUser.class, "favorite_user_id")
            .onUpdate(update -> {
                if (update.oldValue() == null) {
                    return;
                }
                updateCalled.set(updateCalled.get() + 1);
            });
    private SnapchatUser(DataManager dataManager, UUID id) {
        super(dataManager, "snapchat", "users", id);
    }

    public static SnapchatUser createSync(DataManager dataManager) {
        SnapchatUser user = new SnapchatUser(dataManager, UUID.randomUUID());
        SnapchatUserSettings settings = SnapchatUserSettings.createSync(dataManager, user.getId());
        dataManager.insert(user, user.settings.initial(settings));

        return user;
    }

    public SnapchatUserSettings getSettings() {
        return settings.get();
    }

    public @Nullable SnapchatUser getFavoriteUser() {
        return favoriteUser.get();
    }

    public void setFavoriteUser(@Nullable SnapchatUser favoriteUser) {
        this.favoriteUser.set(favoriteUser);
    }

    public Integer getUpdateCalled() {
        return updateCalled.get();
    }
}
