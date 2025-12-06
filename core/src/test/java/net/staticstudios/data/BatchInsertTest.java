package net.staticstudios.data;

import net.staticstudios.data.insert.BatchInsert;
import net.staticstudios.data.insert.PostInsertAction;
import net.staticstudios.data.misc.DataTest;
import net.staticstudios.data.misc.TestUtils;
import net.staticstudios.data.mock.user.MockUser;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

public class BatchInsertTest extends DataTest {

    @Test
    public void testCompletableFuture() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(MockUser.class);

        BatchInsert batch = dataManager.createBatchInsert();
        CompletableFuture<MockUser> cf = MockUser.builder(dataManager)
                .id(UUID.randomUUID())
                .name("zakari")
                .insert(batch);

        assertNotNull(cf);

        batch.insert(InsertMode.SYNC);

        MockUser user = cf.join();
        assertNotNull(user);
    }

    @Test
    public void testManyToManyPostInsertAction() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(MockUser.class);

        List<MockUser> users = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            users.add(MockUser.builder(dataManager)
                    .id(UUID.randomUUID())
                    .name("user" + i)
                    .insert(InsertMode.SYNC));
        }

        UUID userId = UUID.randomUUID();

        BatchInsert batch = dataManager.createBatchInsert();
        CompletableFuture<MockUser> cf = MockUser.builder(dataManager)
                .id(userId)
                .name("zakari")
                .insert(batch);

        assertNotNull(cf);

        for (MockUser user : users) {
            batch.addPostInsertAction(PostInsertAction.manyToMany(dataManager)
                    .referringClass(MockUser.class)
                    .referencedClass(MockUser.class)
                    .joinTableSchema("public")
                    .joinTableName("user_friends")
                    .referringId("id", userId)
                    .referencedId("id", user.id.get())
                    .build());
        }

        batch.insert(InsertMode.SYNC);

        Connection pgConnection = getConnection();

        try (PreparedStatement statement = pgConnection.prepareStatement("SELECT * FROM \"public\".\"user_friends\"")) {
            ResultSet rs = statement.executeQuery();

            assertEquals(users.size(), TestUtils.getResultCount(rs));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        MockUser u = cf.join();

        assertTrue(u.friends.containsAll(users));
    }

    @Test
    public void testManyToManyPostInsertAction2() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(MockUser.class);

        MockUser user = MockUser.builder(dataManager)
                .id(UUID.randomUUID())
                .name("zakari")
                .insert(InsertMode.SYNC);

        List<UUID> friendIds = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            friendIds.add(UUID.randomUUID());
        }

        List<CompletableFuture<MockUser>> friendFutures = new ArrayList<>();
        BatchInsert batch = dataManager.createBatchInsert();
        for (int i = 0; i < 5; i++) {
            friendFutures.add(MockUser.builder(dataManager)
                    .id(friendIds.get(i))
                    .name("friend" + i)
                    .insert(batch));
        }

        for (UUID friendId : friendIds) {
            batch.addPostInsertAction(PostInsertAction.manyToMany(dataManager)
                    .referringClass(MockUser.class)
                    .referencedClass(MockUser.class)
                    .joinTableSchema("public")
                    .joinTableName("user_friends")
                    .referringId("id", user.id.get())
                    .referencedId("id", friendId)
                    .build());
        }

        batch.insert(InsertMode.SYNC);

        assertEquals(friendIds.size(), user.friends.size());

        for (CompletableFuture<MockUser> friendFuture : friendFutures) {
            MockUser friend = friendFuture.join();
            assertTrue(user.friends.contains(friend));
        }
    }
}