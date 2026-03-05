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

        @OneToOne(link = "id=user_id", updateReferencedColumns = true, fkey = false)
        public Reference<UserProfile> profile;
    }
}

