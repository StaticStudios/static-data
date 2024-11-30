package net.staticstudios.data.mocks.reddit;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.Table;
import net.staticstudios.data.UniqueData;
import net.staticstudios.data.UpdatedValue;
import net.staticstudios.data.value.CachedValue;

import java.util.UUID;

/**
 * A mock Reddit user only has a {@link CachedValue} for online status.
 */
@Table("reddit.users")
public class MockRedditUser extends UniqueData {
    private UpdatedValue<Boolean> lastIsOnlineUpdate;
    private final CachedValue<Boolean> isOnline = CachedValue.of(this, Boolean.class, false, "is_online", updated -> lastIsOnlineUpdate = updated);

    @SuppressWarnings("unused")
    private MockRedditUser() {
    }

    public MockRedditUser(DataManager dataManager) {
        super(dataManager, UUID.randomUUID());
        dataManager.insert(this);
    }

    public boolean isOnline() {
        return isOnline.get();
    }

    public void setOnline(boolean isOnline) {
        this.isOnline.set(isOnline);
    }

    public UpdatedValue<Boolean> getLastIsOnlineUpdate() {
        return lastIsOnlineUpdate;
    }
}
