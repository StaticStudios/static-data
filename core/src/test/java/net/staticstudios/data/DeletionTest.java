package net.staticstudios.data;

import net.staticstudios.data.misc.DataTest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class DeletionTest extends DataTest {
    //todo: Similar to this test, we should create a test for update strategies.

    @Test
    public void testReferenceSetNull() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(UserSetNull.class);
        dataManager.finishLoading();

        UserMetadataSetNull metadata = UserMetadataSetNull.builder(dataManager)
                .id(1)
                .insert(InsertMode.SYNC);

        UserSetNull user = UserSetNull.builder(dataManager)
                .id(1)
                .metadataId(1)
                .insert(InsertMode.SYNC);

        user.delete();

        assertFalse(metadata.isDeleted());
    }

    @Test
    public void testReferenceSetNull2() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(UserSetNull.class);
        dataManager.finishLoading();

        UserMetadataSetNull metadata = UserMetadataSetNull.builder(dataManager)
                .id(1)
                .insert(InsertMode.SYNC);

        UserSetNull user = UserSetNull.builder(dataManager)
                .id(1)
                .metadataId(1)
                .insert(InsertMode.SYNC);

        metadata.delete();

        assertFalse(user.isDeleted());
    }

    @Test
    public void testReferenceCascade() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(UserCascade.class);
        dataManager.finishLoading();

        UserMetadataCascade metadata = UserMetadataCascade.builder(dataManager)
                .id(1)
                .insert(InsertMode.SYNC);

        UserCascade user = UserCascade.builder(dataManager)
                .id(1)
                .metadataId(1)
                .insert(InsertMode.SYNC);

        assertSame(metadata, user.ref.get());

        user.delete();

        assertTrue(metadata.isDeleted());
    }

    @Test
    public void testReferenceCascade2() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(UserCascade.class);
        dataManager.finishLoading();

        UserMetadataCascade metadata = UserMetadataCascade.builder(dataManager)
                .id(1)
                .insert(InsertMode.SYNC);

        UserCascade user = UserCascade.builder(dataManager)
                .id(1)
                .metadataId(1)
                .insert(InsertMode.SYNC);

        assertSame(metadata, user.ref.get());

        metadata.delete();

        assertFalse(user.isDeleted());
    }

    @Test
    public void testOneToManySetNull() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(UserSetNull.class);
        dataManager.finishLoading();

        UserSetNull user = UserSetNull.builder(dataManager)
                .id(1)
                .insert(InsertMode.SYNC);

        UserActionSetNull action1 = UserActionSetNull.builder(dataManager)
                .id(1)
                .userId(1)
                .insert(InsertMode.SYNC);

        user.delete();

        assertFalse(action1.isDeleted());
        assertNull(action1.userId.get());
    }

    @Test
    public void testOneToManySetNull2() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(UserSetNull.class);
        dataManager.finishLoading();

        UserSetNull user = UserSetNull.builder(dataManager)
                .id(1)
                .insert(InsertMode.SYNC);

        UserActionSetNull action1 = UserActionSetNull.builder(dataManager)
                .id(1)
                .userId(1)
                .insert(InsertMode.SYNC);

        action1.delete();

        assertFalse(user.isDeleted());
    }

    @Test
    public void testOneToManyCascade() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(UserCascade.class);
        dataManager.finishLoading();

        UserCascade user = UserCascade.builder(dataManager)
                .id(1)
                .insert(InsertMode.SYNC);

        UserActionCascade action1 = UserActionCascade.builder(dataManager)
                .id(1)
                .userId(1)
                .insert(InsertMode.SYNC);

        user.delete();

        assertFalse(action1.isDeleted());
        assertNull(action1.userId.get());
    }

    @Test
    public void testOneToManyCascade2() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(UserCascade.class);
        dataManager.finishLoading();

        UserCascade user = UserCascade.builder(dataManager)
                .id(1)
                .insert(InsertMode.SYNC);

        UserActionCascade action1 = UserActionCascade.builder(dataManager)
                .id(1)
                .userId(1)
                .insert(InsertMode.SYNC);

        action1.delete();

        assertFalse(user.isDeleted());
    }

    //todo: many-to-many collections test and impl.


    @Data(schema = "test", table = "user_metadata")
    static class UserMetadataSetNull extends UniqueData {
        @IdColumn(name = "id")
        public PersistentValue<Integer> id;
    }

    @Data(schema = "test", table = "user_actions")
    static class UserActionSetNull extends UniqueData {
        @IdColumn(name = "id")
        public PersistentValue<Integer> id;

        @Column(name = "user_id", nullable = true)
        public PersistentValue<Integer> userId;
    }

    @Data(schema = "test", table = "users")
    static class UserSetNull extends UniqueData {

        @IdColumn(name = "id")
        public PersistentValue<Integer> id;

        @Column(name = "metadata_id", nullable = true)
        public PersistentValue<Integer> metadataId;

        @OneToOne(link = "metadata_id=id")
        @Delete(DeleteStrategy.SET_NULL)
        public Reference<UserMetadataSetNull> ref;

        @OneToMany(link = "id=user_id")
        @Delete(DeleteStrategy.SET_NULL)
        public PersistentCollection<UserActionSetNull> actions;
    }

    @Data(schema = "test", table = "user_metadata")
    static class UserMetadataCascade extends UniqueData {
        @IdColumn(name = "id")
        public PersistentValue<Integer> id;
    }

    @Data(schema = "test", table = "user_actions")
    static class UserActionCascade extends UniqueData {
        @IdColumn(name = "id")
        public PersistentValue<Integer> id;

        @Column(name = "user_id", nullable = true)
        public PersistentValue<Integer> userId;
    }

    @Data(schema = "test", table = "users")
    static class UserCascade extends UniqueData {

        @IdColumn(name = "id")
        public PersistentValue<Integer> id;

        @Column(name = "metadata_id", nullable = true)
        public PersistentValue<Integer> metadataId;

        @OneToOne(link = "metadata_id=id")
        @Delete(DeleteStrategy.CASCADE)
        public Reference<UserMetadataCascade> ref;

        @OneToMany(link = "id=user_id")
        @Delete(DeleteStrategy.SET_NULL)
        public PersistentCollection<UserActionCascade> actions;
    }
}
