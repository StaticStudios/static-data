package net.staticstudios.data.mocks.facebook;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.Table;
import net.staticstudios.data.UniqueData;
import net.staticstudios.data.value.PersistentCollection;
import org.jetbrains.annotations.Blocking;

import java.util.Collection;
import java.util.UUID;

/**
 * A mock Facebook user only has {@link PersistentCollection}
 */
@Table("facebook.users")
public class MockFacebookUser extends UniqueData {
    private MockFacebookFriendEntry lastFriendUpdated;
    private MockFacebookFriendEntry lastFriendRemoved;
    private MockFacebookFriendEntry lastFriendAdded;
    private final PersistentCollection<MockFacebookFriendEntry> friends = PersistentCollection.of(this, MockFacebookFriendEntry.class, "facebook.friends", "id")
            .onAdd(friend -> lastFriendAdded = friend)
            .onRemove(friend -> lastFriendRemoved = friend)
            .onUpdate(friend -> lastFriendUpdated = friend);

    @SuppressWarnings("unused")
    private MockFacebookUser() {
    }

    @Blocking
    public MockFacebookUser(DataManager dataManager) {
        super(dataManager, UUID.randomUUID());
        dataManager.insert(this);
    }

    @Blocking
    public MockFacebookUser(DataManager dataManager, Collection<MockFacebookFriendEntry> initialFriends) {
        super(dataManager, UUID.randomUUID());

        friends.setInitialElements(initialFriends);
        dataManager.insert(this);
    }

    public PersistentCollection<MockFacebookFriendEntry> getFriends() {
        return friends;
    }

    public void addFriend(UUID id) {
        friends.add(new MockFacebookFriendEntry(getDataManager(), id));
    }

    public void removeFriend(UUID id) {
        friends.removeIf(friend -> friend.getId().equals(id));
    }

    public MockFacebookFriendEntry getLastFriendUpdated() {
        return lastFriendUpdated;
    }

    public MockFacebookFriendEntry getLastFriendRemoved() {
        return lastFriendRemoved;
    }

    public MockFacebookFriendEntry getLastFriendAdded() {
        return lastFriendAdded;
    }
}
