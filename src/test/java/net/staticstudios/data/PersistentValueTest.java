package net.staticstudios.data;

import net.staticstudios.data.misc.DataTest;
import net.staticstudios.data.misc.MockEnvironment;
import net.staticstudios.data.mock.MockUser;
import net.staticstudios.data.mock.MockUserFactory;
import net.staticstudios.data.util.ColumnValuePair;
import org.junit.jupiter.api.Test;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

public class PersistentValueTest extends DataTest {

    @Test
    public void testReadData() throws SQLException {
        List<UUID> userIds = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            userIds.add(UUID.randomUUID());
        }

        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(MockUser.class);
        for (UUID id : userIds) {
            MockUserFactory.builder(dataManager)
                    .id(id)
                    .name("user " + id)
                    .insert(InsertMode.SYNC);
        }

        for (UUID id : userIds) {
            MockUser user = dataManager.getInstance(MockUser.class, ColumnValuePair.of("id", id));
            assertEquals("user " + id, user.name.get());
            assertNull(user.age.get());
        }

        waitForDataPropagation();

        MockEnvironment environment2 = createMockEnvironment();
        DataManager dataManager2 = environment2.dataManager();
        dataManager2.load(MockUser.class);
        for (UUID id : userIds) {
            MockUser user = dataManager2.getInstance(MockUser.class, ColumnValuePair.of("id", id));
            assertEquals("user " + id, user.name.get());
            assertNull(user.age.get());
        }
    }

    @Test
    public void test() throws SQLException {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(MockUser.class);
        UUID id = UUID.randomUUID();
        MockUser mockUser = MockUserFactory.builder(dataManager)
                .id(id)
                .name("test user")
                .nameUpdates(0)
                .insert(InsertMode.SYNC);
        assertEquals("test user", mockUser.name.get());
        mockUser.name.set("updated name");
        assertEquals("updated name", mockUser.name.get());

        assertNull(mockUser.age.get());
        mockUser.age.set(25);
        assertEquals(25, mockUser.age.get());

        mockUser = dataManager.getInstance(MockUser.class, ColumnValuePair.of("id", id));
        mockUser = null; // remove strong reference
        System.gc();
        mockUser = dataManager.getInstance(MockUser.class, ColumnValuePair.of("id", id)); // should have a cache miss

        assertNull(mockUser.favoriteColor.get());
        mockUser.favoriteColor.set("blue");
        assertEquals("blue", mockUser.favoriteColor.get());

//        long start;
//        int count = 10_000;
//        for (int j = 0; j < 5; j++) {
//            start = System.currentTimeMillis();
//            for (int i = 0; i < count; i++) {
//                mockUser.name.set("name " + i);
//            }
//
//            System.out.println("Took " + (System.currentTimeMillis() - start) + "ms to do " + count + " updates");
//        }
//        for (int j = 0; j < 5; j++) {
//            start = System.currentTimeMillis();
//            for (int i = 0; i < count; i++) {
//                mockUser.name.get();
//            }
//            System.out.println("Took " + (System.currentTimeMillis() - start) + "ms to do " + count + " gets");
//        }

        waitForDataPropagation();
    }

    @Test
    public void testUpdateHandlerRegistration() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(MockUser.class);
        UUID id = UUID.randomUUID();
        assertEquals(0, dataManager.getUpdateHandlers("public", "users", "name", MockUser.class).size());
        MockUser mockUser = MockUserFactory.builder(dataManager)
                .id(id)
                .name("test user")
                .favoriteColor("orange")
                .nameUpdates(0)
                .insert(InsertMode.SYNC);
        assertEquals("test user", mockUser.name.get());
        //first instance was created, handler should be registered
        assertEquals(1, dataManager.getUpdateHandlers("public", "users", "name", MockUser.class).size());
        mockUser = null; // remove strong reference
        System.gc();
        mockUser = dataManager.getInstance(MockUser.class, ColumnValuePair.of("id", id)); // should have a cache miss
        //the handler for this pv should not have been registered again
        assertEquals(1, dataManager.getUpdateHandlers("public", "users", "name", MockUser.class).size());
        waitForUpdateHandlers();
        assertEquals(0, mockUser.getNameUpdates());

        mockUser.name.set("new name");
        waitForUpdateHandlers();
        assertEquals(1, mockUser.getNameUpdates());
        mockUser.name.set("new name");
        waitForUpdateHandlers();
        assertEquals(1, mockUser.getNameUpdates());
        mockUser.name.set("new name2");
        waitForUpdateHandlers();
        assertEquals(2, mockUser.getNameUpdates());
        mockUser.name.set("new name");
        waitForUpdateHandlers();
        assertEquals(3, mockUser.getNameUpdates());
    }

    @Test
    public void testReceiveUpdateFromPostgres() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(MockUser.class);
        UUID id = UUID.randomUUID();
        MockUser mockUser = MockUserFactory.builder(dataManager)
                .id(id)
                .name("test user")
                .favoriteColor("orange")
                .nameUpdates(0)
                .insert(InsertMode.SYNC);

        assertEquals("test user", mockUser.name.get());
        assertEquals(0, mockUser.getNameUpdates());

        try (PreparedStatement preparedStatement = getConnection().prepareStatement("UPDATE users SET name = ? WHERE id = ?")) {
            preparedStatement.setString(1, "updated from pg");
            preparedStatement.setObject(2, id);
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        waitForDataPropagation();

        assertEquals("updated from pg", mockUser.name.get());
        assertEquals(1, mockUser.getNameUpdates());
    }

    @Test
    public void testReceiveInsertFromPostgres() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(MockUser.class);
        UUID id = UUID.randomUUID();

        try (PreparedStatement preferencesStatement = getConnection().prepareStatement("INSERT INTO user_preferences (user_id, fav_color) VALUES (?, ?)");
             PreparedStatement metadataStatement = getConnection().prepareStatement("INSERT INTO user_metadata (user_id, name_updates) VALUES (?, ?)");
             PreparedStatement userStatement = getConnection().prepareStatement("INSERT INTO users (id, name) VALUES (?, ?)")) {
            userStatement.setObject(1, id);
            userStatement.setString(2, "inserted from pg");
            userStatement.executeUpdate();
            preferencesStatement.setObject(1, id);
            preferencesStatement.setObject(2, 0);
            preferencesStatement.executeUpdate();
            metadataStatement.setObject(1, id);
            metadataStatement.setInt(2, 0);
            metadataStatement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        waitForDataPropagation();

        MockUser mockUser = dataManager.getInstance(MockUser.class, ColumnValuePair.of("id", id));

        assertEquals("inserted from pg", mockUser.name.get());
        assertEquals(0, mockUser.getNameUpdates());
    }

    @Test
    public void testReceiveDeleteFromPostgres() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(MockUser.class);
        UUID id = UUID.randomUUID();
        MockUser mockUser = MockUserFactory.builder(dataManager)
                .id(id)
                .name("test user")
                .favoriteColor("orange")
                .nameUpdates(0)
                .insert(InsertMode.SYNC);

        assertEquals("test user", mockUser.name.get());
        assertEquals(0, mockUser.getNameUpdates());
        assertFalse(mockUser.isDeleted());

        try (PreparedStatement preparedStatement = getConnection().prepareStatement("DELETE FROM users WHERE id = ?")) {
            preparedStatement.setObject(1, id);
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        waitForDataPropagation();

        assertTrue(mockUser.isDeleted());

        mockUser = dataManager.getInstance(MockUser.class, ColumnValuePair.of("id", id));
        assertNull(mockUser);
    }

    @Test
    public void testChangeIdColumn() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(MockUser.class);
        UUID id = UUID.randomUUID();
        MockUser mockUser = MockUserFactory.builder(dataManager)
                .id(id)
                .name("test user")
                .favoriteColor("orange")
                .nameUpdates(0)
                .insert(InsertMode.SYNC);

        assertEquals(id, mockUser.id.get());
        assertEquals(0, mockUser.nameUpdates.get());
        mockUser.name.set("new name");
        waitForUpdateHandlers();
        assertEquals(1, mockUser.nameUpdates.get());
        UUID newId = UUID.randomUUID();
        mockUser.id.set(newId);
        assertEquals(newId, mockUser.id.get());
        assertNull(dataManager.getInstance(MockUser.class, ColumnValuePair.of("id", id)));
        assertNotNull(dataManager.getInstance(MockUser.class, ColumnValuePair.of("id", newId)));
        assertSame(dataManager.getInstance(MockUser.class, ColumnValuePair.of("id", newId)), mockUser);
        assertEquals(1, mockUser.nameUpdates.get());
        mockUser.name.set("new name2");
        waitForUpdateHandlers();
        assertEquals(2, mockUser.nameUpdates.get());
    }

    @Test
    public void testChangeIdColumnInPostgres() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(MockUser.class);
        UUID id = UUID.randomUUID();
        MockUser mockUser = MockUserFactory.builder(dataManager)
                .id(id)
                .name("test user")
                .favoriteColor("orange")
                .nameUpdates(0)
                .insert(InsertMode.SYNC);

        assertEquals(id, mockUser.id.get());
        assertEquals(0, mockUser.nameUpdates.get());
        mockUser.name.set("new name");

        waitForUpdateHandlers();

        assertEquals(1, mockUser.nameUpdates.get());
        UUID newId = UUID.randomUUID();

        try (PreparedStatement preparedStatement = getConnection().prepareStatement("UPDATE users SET id = ? WHERE id = ?")) {
            preparedStatement.setObject(1, newId);
            preparedStatement.setObject(2, id);
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        waitForDataPropagation();

        assertEquals(newId, mockUser.id.get());
        assertNull(dataManager.getInstance(MockUser.class, ColumnValuePair.of("id", id)));
        assertNotNull(dataManager.getInstance(MockUser.class, ColumnValuePair.of("id", newId)));
        assertSame(dataManager.getInstance(MockUser.class, ColumnValuePair.of("id", newId)), mockUser);
        waitForUpdateHandlers();
        assertEquals(1, mockUser.nameUpdates.get());
        mockUser.name.set("new name2");
        waitForUpdateHandlers();
        assertEquals(2, mockUser.nameUpdates.get());
    }
}