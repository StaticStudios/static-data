package net.staticstudios.data;

import net.staticstudios.data.misc.DataTest;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

public class ReferenceUpdateReferencedColumnsTest extends DataTest {

    @Test
    public void testSetReferenceUpdatesReferencedTable() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(UserWithProfile.class);
        dataManager.finishLoading();

        UserWithProfile user = UserWithProfile.builder(dataManager)
                .id(UUID.randomUUID())
                .insert(InsertMode.SYNC);

        UserProfile profile = UserProfile.builder(dataManager)
                .id(UUID.randomUUID())
                .insert(InsertMode.SYNC);

        assertNull(user.profile.get());
        assertNull(profile.userId.get());

        user.profile.set(profile);

        assertSame(profile, user.profile.get());
        assertEquals(user.id.get(), profile.userId.get());
    }

    @Test
    public void testSetReferenceToNullUnlinksReferencedTable() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(UserWithProfile.class);
        dataManager.finishLoading();

        UUID userId = UUID.randomUUID();
        UUID profileId = UUID.randomUUID();

        UserProfile profile = UserProfile.builder(dataManager)
                .id(profileId)
                .userId(userId)
                .insert(InsertMode.SYNC);

        UserWithProfile user = UserWithProfile.builder(dataManager)
                .id(userId)
                .insert(InsertMode.SYNC);

        assertSame(profile, user.profile.get());

        user.profile.set(null);

        assertNull(user.profile.get());
        assertNull(profile.userId.get());
    }

    @Test
    public void testChangeReferenceUpdatesReferencedTable() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(UserWithProfile.class);
        dataManager.finishLoading();

        UserWithProfile user = UserWithProfile.builder(dataManager)
                .id(UUID.randomUUID())
                .insert(InsertMode.SYNC);

        UserProfile profile1 = UserProfile.builder(dataManager)
                .id(UUID.randomUUID())
                .insert(InsertMode.SYNC);

        UserProfile profile2 = UserProfile.builder(dataManager)
                .id(UUID.randomUUID())
                .insert(InsertMode.SYNC);

        user.profile.set(profile1);
        assertSame(profile1, user.profile.get());
        assertEquals(user.id.get(), profile1.userId.get());

        user.profile.set(profile2);
        assertSame(profile2, user.profile.get());
        assertEquals(user.id.get(), profile2.userId.get());
        assertNull(profile1.userId.get());
    }

    @Test
    public void testSetReferenceToDifferentUserUnlinksPrevious() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(UserWithProfile.class);
        dataManager.finishLoading();

        UserWithProfile user1 = UserWithProfile.builder(dataManager)
                .id(UUID.randomUUID())
                .insert(InsertMode.SYNC);

        UserWithProfile user2 = UserWithProfile.builder(dataManager)
                .id(UUID.randomUUID())
                .insert(InsertMode.SYNC);

        UserProfile profile = UserProfile.builder(dataManager)
                .id(UUID.randomUUID())
                .insert(InsertMode.SYNC);

        user1.profile.set(profile);
        assertSame(profile, user1.profile.get());
        assertEquals(user1.id.get(), profile.userId.get());

        user2.profile.set(profile);
        assertSame(profile, user2.profile.get());
        assertEquals(user2.id.get(), profile.userId.get());
        assertNull(user1.profile.get());
    }

    @Test
    public void testUpdateHandlerCalledOnSet() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(UserWithProfile.class);
        dataManager.finishLoading();

        UserWithProfile user = UserWithProfile.builder(dataManager)
                .id(UUID.randomUUID())
                .insert(InsertMode.SYNC);

        UserProfile profile = UserProfile.builder(dataManager)
                .id(UUID.randomUUID())
                .insert(InsertMode.SYNC);

        assertEquals(0, user.profileUpdates.get());

        user.profile.set(profile);
        assertEquals(1, user.profileUpdates.get());

        user.profile.set(null);
        assertEquals(2, user.profileUpdates.get());
    }

    @Test
    public void testUpdateHandlerNotCalledOnRedundantSet() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(UserWithProfile.class);
        dataManager.finishLoading();

        UserWithProfile user = UserWithProfile.builder(dataManager)
                .id(UUID.randomUUID())
                .insert(InsertMode.SYNC);

        assertEquals(0, user.profileUpdates.get());

        user.profile.set(null);
        assertEquals(0, user.profileUpdates.get());
    }

    @Test
    public void testUpdateHandlerCalledOnChange() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(UserWithProfile.class);
        dataManager.finishLoading();

        UserWithProfile user = UserWithProfile.builder(dataManager)
                .id(UUID.randomUUID())
                .insert(InsertMode.SYNC);

        UserProfile profile1 = UserProfile.builder(dataManager)
                .id(UUID.randomUUID())
                .insert(InsertMode.SYNC);

        UserProfile profile2 = UserProfile.builder(dataManager)
                .id(UUID.randomUUID())
                .insert(InsertMode.SYNC);

        assertEquals(0, user.profileUpdates.get());

        user.profile.set(profile1);
        assertEquals(1, user.profileUpdates.get());


        //TODO: currently this triggers the handler to be called twice. Once for the unlink of profile1, and once for the link of profile2.
        // This is not ideal, but it is a consequence of how the update handlers are currently implemented.
        // To properly fix this, we need to store information in the holder table (in h2) which tells us the last value
        // that was called for this update handler, and if the new value is the same as the last value, we can skip calling the handler again.
        // We should do this in h2 otherwise when an instance is deleted, we have to properly cleanup after it (we cannot store it in the UniqueData class as that can be GCed).
        // Plus storing in h2 allows us to support a disk based cache rather than just an in memory one.
        user.profile.set(profile2);
        assertEquals(3, user.profileUpdates.get());

        user.profile.set(null);
        assertEquals(4, user.profileUpdates.get());
    }

    @Test
    public void testUpdateHandlerCalledOnReassignToDifferentUser() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(UserWithProfile.class);
        dataManager.finishLoading();

        UserWithProfile user1 = UserWithProfile.builder(dataManager)
                .id(UUID.randomUUID())
                .insert(InsertMode.SYNC);

        UserWithProfile user2 = UserWithProfile.builder(dataManager)
                .id(UUID.randomUUID())
                .insert(InsertMode.SYNC);

        UserProfile profile = UserProfile.builder(dataManager)
                .id(UUID.randomUUID())
                .insert(InsertMode.SYNC);

        assertEquals(0, user1.profileUpdates.get());
        assertEquals(0, user2.profileUpdates.get());

        user1.profile.set(profile);
        assertEquals(1, user1.profileUpdates.get());
        assertEquals(0, user2.profileUpdates.get());

        user2.profile.set(profile);
        assertEquals(2, user1.profileUpdates.get());
        assertEquals(1, user2.profileUpdates.get());
    }

    @Test
    public void testUpdateHandlerCalledOnProfileDelete() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(UserWithProfile.class);
        dataManager.finishLoading();

        UserWithProfile user = UserWithProfile.builder(dataManager)
                .id(UUID.randomUUID())
                .insert(InsertMode.SYNC);
        UserProfile profile = UserProfile.builder(dataManager)
                .id(UUID.randomUUID())
                .insert(InsertMode.SYNC);

        user.profile.set(profile);
        assertEquals(1, user.profileUpdates.get());

        profile.delete();
        assertNull(user.profile.get());
        assertEquals(2, user.profileUpdates.get());
    }

    @Test
    public void testUpdateHandlerCalledOnProfileInsertWithLinkingColumnSet() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(UserWithProfile.class);
        dataManager.finishLoading();

        UserWithProfile user = UserWithProfile.builder(dataManager)
                .id(UUID.randomUUID())
                .insert(InsertMode.SYNC);

        assertEquals(0, user.profileUpdates.get());

        UserProfile profile = UserProfile.builder(dataManager)
                .id(UUID.randomUUID())
                .userId(user.id.get())
                .insert(InsertMode.SYNC);

        assertSame(profile, user.profile.get());
        assertEquals(1, user.profileUpdates.get());
    }

    @Data(schema = "test_urc", table = "user_profiles")
    static class UserProfile extends UniqueData {
        @IdColumn(name = "id")
        public PersistentValue<UUID> id;

        @Column(name = "user_id", nullable = true, unique = true)
        public PersistentValue<UUID> userId;
    }

    @Data(schema = "test_urc", table = "users")
    static class UserWithProfile extends UniqueData {
        @IdColumn(name = "id")
        public PersistentValue<UUID> id;

        @Identifier("profile_updates")
        public CachedValue<Integer> profileUpdates = CachedValue.of(this, Integer.class)
                .withFallback(0);

        @OneToOne(link = "id=user_id", updateReferencedColumns = true, fkey = false)
        public Reference<UserProfile> profile = Reference.of(this, UserProfile.class)
                .onUpdate(UserWithProfile.class, (user, update) -> user.profileUpdates.set(user.profileUpdates.get() + 1));
    }
}

