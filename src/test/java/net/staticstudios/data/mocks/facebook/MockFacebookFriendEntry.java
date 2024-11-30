package net.staticstudios.data.mocks.facebook;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.shared.CollectionEntry;
import net.staticstudios.data.value.PersistentEntryValue;

import java.util.UUID;

public class MockFacebookFriendEntry extends CollectionEntry {
    private final PersistentEntryValue<UUID> friendId = PersistentEntryValue.immutable(this, UUID.class, "friend_id");
    private final PersistentEntryValue<Boolean> favorite = PersistentEntryValue.mutable(this, Boolean.class, "favorite");

    @SuppressWarnings("unused")
    private MockFacebookFriendEntry() {
    }

    public MockFacebookFriendEntry(DataManager dataManager, UUID id) {
        super(dataManager);
        this.friendId.setInitialValue(id);
        this.favorite.setInitialValue(false);
    }

    public UUID getId() {
        return friendId.get();
    }

    public void setId(UUID id) {
        this.friendId.set(id);
    }

    public boolean isFavorite() {
        return favorite.get();
    }

    public void setFavorite(boolean favorite) {
        this.favorite.set(favorite);
    }
}
