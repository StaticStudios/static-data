package net.staticstudios.data;

import net.staticstudios.data.misc.DataTest;
import net.staticstudios.data.misc.MockEnvironment;
import net.staticstudios.data.mock.user.MockUser;
import net.staticstudios.data.util.ColumnValuePair;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.lang.ref.WeakReference;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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
            MockUser.builder(dataManager)
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
    public void testUniqueDataCache() throws SQLException {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(MockUser.class);
        UUID id = UUID.randomUUID();
        MockUser mockUser = MockUser.builder(dataManager)
                .id(id)
                .name("test user")
                .nameUpdates(0)
                .insert(InsertMode.SYNC);

        WeakReference<MockUser> weakRef = new WeakReference<>(mockUser);

        System.gc();

        assertNotNull(weakRef.get());

        mockUser = dataManager.getInstance(MockUser.class, ColumnValuePair.of("id", id));
        assertSame(mockUser, weakRef.get());
        mockUser = null; // remove strong reference
        System.gc();

        assertNull(weakRef.get());

        mockUser = dataManager.getInstance(MockUser.class, ColumnValuePair.of("id", id)); // should have a cache miss
    }

    @Test
    public void testUpdate() throws SQLException {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(MockUser.class);
        UUID id = UUID.randomUUID();
        MockUser mockUser = MockUser.builder(dataManager)
                .id(id)
                .name("test user")
                .age(0)
                .insert(InsertMode.SYNC);

        assertEquals(0, mockUser.age.get());

        Connection h2Connection = getH2Connection(dataManager);
        try (PreparedStatement preparedStatement = h2Connection.prepareStatement("SELECT \"age\" FROM \"public\".\"users\" WHERE \"id\" = ?")) {
            preparedStatement.setObject(1, id);
            ResultSet rs = preparedStatement.executeQuery();
            assertTrue(rs.next());
            assertEquals(0, rs.getObject("age"));
        }

        waitForDataPropagation();

        Connection pgConnection = getConnection();
        try (PreparedStatement preparedStatement = pgConnection.prepareStatement("SELECT \"age\" FROM \"public\".\"users\" WHERE \"id\" = ?")) {
            preparedStatement.setObject(1, id);
            ResultSet rs = preparedStatement.executeQuery();
            assertTrue(rs.next());
            assertEquals(0, rs.getObject("age"));
        }

        mockUser.age.set(30);

        try (PreparedStatement preparedStatement = h2Connection.prepareStatement("SELECT \"age\" FROM \"public\".\"users\" WHERE \"id\" = ?")) {
            preparedStatement.setObject(1, id);
            ResultSet rs = preparedStatement.executeQuery();
            assertTrue(rs.next());
            assertEquals(30, rs.getObject("age"));
        }

        waitForDataPropagation();

        try (PreparedStatement preparedStatement = pgConnection.prepareStatement("SELECT \"age\" FROM \"public\".\"users\" WHERE \"id\" = ?")) {
            preparedStatement.setObject(1, id);
            ResultSet rs = preparedStatement.executeQuery();
            assertTrue(rs.next());
            assertEquals(30, rs.getObject("age"));
        }
    }

    @Test
    public void testUpdateForeignColumn() throws SQLException {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(MockUser.class);
        UUID id = UUID.randomUUID();
        MockUser mockUser = MockUser.builder(dataManager)
                .id(id)
                .name("test user")
                .favoriteColor("blue")
                .insert(InsertMode.SYNC);

        assertEquals("blue", mockUser.favoriteColor.get());

        Connection h2Connection = getH2Connection(dataManager);
        try (PreparedStatement preparedStatement = h2Connection.prepareStatement("SELECT \"fav_color\" FROM \"public\".\"user_preferences\" WHERE \"user_id\" = ?")) {
            preparedStatement.setObject(1, id);
            ResultSet rs = preparedStatement.executeQuery();
            assertTrue(rs.next());
            assertEquals("blue", rs.getObject("fav_color"));
        }

        waitForDataPropagation();

        Connection pgConnection = getConnection();
        try (PreparedStatement preparedStatement = pgConnection.prepareStatement("SELECT \"fav_color\" FROM \"public\".\"user_preferences\" WHERE \"user_id\" = ?")) {
            preparedStatement.setObject(1, id);
            ResultSet rs = preparedStatement.executeQuery();
            assertTrue(rs.next());
            assertEquals("blue", rs.getObject("fav_color"));
        }

        mockUser.favoriteColor.set("red");

        try (PreparedStatement preparedStatement = h2Connection.prepareStatement("SELECT \"fav_color\" FROM \"public\".\"user_preferences\" WHERE \"user_id\" = ?")) {
            preparedStatement.setObject(1, id);
            ResultSet rs = preparedStatement.executeQuery();
            assertTrue(rs.next());
            assertEquals("red", rs.getObject("fav_color"));
        }

        waitForDataPropagation();

        try (PreparedStatement preparedStatement = pgConnection.prepareStatement("SELECT \"fav_color\" FROM \"public\".\"user_preferences\" WHERE \"user_id\" = ?")) {
            preparedStatement.setObject(1, id);
            ResultSet rs = preparedStatement.executeQuery();
            assertTrue(rs.next());
            assertEquals("red", rs.getObject("fav_color"));
        }
    }

    @Test
    public void testUpdateHandlerRegistration() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(MockUser.class);
        UUID id = UUID.randomUUID();
        assertEquals(0, dataManager.getUpdateHandlers("public", "users", "name", MockUser.class).size());
        MockUser mockUser = MockUser.builder(dataManager)
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
        MockUser mockUser = MockUser.builder(dataManager)
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
            preferencesStatement.setObject(1, id);
            preferencesStatement.setObject(2, 0);
            preferencesStatement.executeUpdate();
            metadataStatement.setObject(1, id);
            metadataStatement.setInt(2, 0);
            metadataStatement.executeUpdate();
            userStatement.setObject(1, id);
            userStatement.setString(2, "inserted from pg");
            userStatement.executeUpdate();
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
        MockUser mockUser = MockUser.builder(dataManager)
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

    @Disabled //todo: known to break
    @Test
    public void testChangeIdColumn() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(MockUser.class);
        UUID id = UUID.randomUUID();
        MockUser mockUser = MockUser.builder(dataManager)
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

    @Disabled //todo: known to break
    @Test
    public void testChangeIdColumnInPostgres() {
        //todo: this and the other id column test are failing because fkeys have been changed to be on the user referringTable. use a trigger to update the fkeys on id change, similar to the cascade delete trigger
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(MockUser.class);
        UUID id = UUID.randomUUID();
        MockUser mockUser = MockUser.builder(dataManager)
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

    @Test
    public void testUpdateInterval() throws Exception {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(MockUser.class);
        UUID id = UUID.randomUUID();
        MockUser mockUser = MockUser.builder(dataManager)
                .id(id)
                .name("test user")
                .favoriteColor("orange")
                .nameUpdates(0)
                .insert(InsertMode.SYNC);

        assertNull(mockUser.views.get());

        for (int i = 0; i < 5; i++) {
            mockUser.views.set(i);
        }

        assertEquals(4, mockUser.views.get());
        waitForDataPropagation();
        Connection connection = getConnection();
        try (PreparedStatement preparedStatement = connection.prepareStatement("SELECT views FROM users WHERE id = ?")) {
            preparedStatement.setObject(1, id);
            ResultSet rs = preparedStatement.executeQuery();
            assertTrue(rs.next());
            assertNull(rs.getObject("views"));
        }

        Thread.sleep(6000);
        try (PreparedStatement preparedStatement = connection.prepareStatement("SELECT views FROM users WHERE id = ?")) {
            preparedStatement.setObject(1, id);
            ResultSet rs = preparedStatement.executeQuery();
            assertTrue(rs.next());
            assertEquals(4, rs.getInt("views"));
        }
    }

    @Test
    public void testInsertStrategyPreferExisting() throws SQLException {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(MockUser.class);
        Connection connection = getConnection();
        UUID id = UUID.randomUUID();
        try (PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO public.user_preferences (user_id, fav_color) VALUES (?, ?)")) {
            preparedStatement.setObject(1, id);
            preparedStatement.setString(2, "blue");
            preparedStatement.executeUpdate();
        }

        MockUser user1 = MockUser.builder(dataManager)
                .id(UUID.randomUUID())
                .name("test user")
                .favoriteColor("red")
                .insert(InsertMode.SYNC);
        assertEquals("red", user1.favoriteColor.get());
        MockUser user2 = MockUser.builder(dataManager)
                .id(id)
                .name("test user2")
                .favoriteColor("green")
                .insert(InsertMode.SYNC);
        assertEquals("blue", user2.favoriteColor.get());

        waitForDataPropagation();

        try (PreparedStatement preparedStatement = connection.prepareStatement("SELECT fav_color FROM public.user_preferences WHERE user_id = ?")) {
            preparedStatement.setObject(1, id);
            ResultSet rs = preparedStatement.executeQuery();
            assertTrue(rs.next());
            assertEquals("blue", rs.getString("fav_color"));
        }
    }

    @Test
    public void testInsertStrategyOverwriteExisting() throws SQLException {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(MockUser.class);
        Connection connection = getConnection();
        UUID id = UUID.randomUUID();
        try (PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO public.user_metadata (user_id, name_updates) VALUES (?, ?)")) {
            preparedStatement.setObject(1, id);
            preparedStatement.setInt(2, 5);
            preparedStatement.executeUpdate();
        }
        MockUser user1 = MockUser.builder(dataManager)
                .id(UUID.randomUUID())
                .name("test user")
                .nameUpdates(10)
                .insert(InsertMode.SYNC);
        assertEquals(10, user1.nameUpdates.get());
        MockUser user2 = MockUser.builder(dataManager)
                .id(id)
                .name("test user2")
                .nameUpdates(15)
                .insert(InsertMode.SYNC);
        assertEquals(15, user2.nameUpdates.get());

        waitForDataPropagation();

        try (PreparedStatement preparedStatement = connection.prepareStatement("SELECT name_updates FROM public.user_metadata WHERE user_id = ?")) {
            preparedStatement.setObject(1, id);
            ResultSet rs = preparedStatement.executeQuery();
            assertTrue(rs.next());
            assertEquals(15, rs.getInt("name_updates"));
        }
    }

    @Test
    public void testDeleteStrategyCascade() throws SQLException {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(MockUser.class);
        Connection h2Connection = getH2Connection(dataManager);
        Connection pgConnection = getConnection();
        UUID id = UUID.randomUUID();
        MockUser user = MockUser.builder(dataManager)
                .id(id)
                .name("test user")
                .favoriteColor("red")
                .nameUpdates(0)
                .insert(InsertMode.SYNC);
        assertEquals("red", user.favoriteColor.get());
        try (PreparedStatement preparedStatement = h2Connection.prepareStatement("SELECT \"fav_color\" FROM \"public\".\"user_preferences\" WHERE \"user_id\" = ?")) {
            preparedStatement.setObject(1, id);
            ResultSet rs = preparedStatement.executeQuery();
            assertTrue(rs.next());
            assertEquals("red", rs.getString("fav_color"));
        }

        waitForDataPropagation();

        try (PreparedStatement preparedStatement = pgConnection.prepareStatement("SELECT \"fav_color\" FROM \"public\".\"user_preferences\" WHERE \"user_id\" = ?")) {
            preparedStatement.setObject(1, id);
            ResultSet rs = preparedStatement.executeQuery();
            assertTrue(rs.next());
            assertEquals("red", rs.getString("fav_color"));
        }

        user.delete();
        assertTrue(user.isDeleted());

        try (PreparedStatement preparedStatement = h2Connection.prepareStatement("SELECT * FROM \"public\".\"user_preferences\" WHERE \"user_id\" = ?")) {
            preparedStatement.setObject(1, id);
            ResultSet rs = preparedStatement.executeQuery();
            assertFalse(rs.next());
        }

        waitForDataPropagation();

        try (PreparedStatement preparedStatement = pgConnection.prepareStatement("SELECT * FROM \"public\".\"user_preferences\" WHERE \"user_id\" = ?")) {
            preparedStatement.setObject(1, id);
            ResultSet rs = preparedStatement.executeQuery();
            assertFalse(rs.next());
        }
    }

    @Test
    public void testDeleteStrategyNoAction() throws SQLException {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(MockUser.class);
        Connection h2Connection = getH2Connection(dataManager);
        Connection pgConnection = getConnection();
        UUID id = UUID.randomUUID();
        MockUser user = MockUser.builder(dataManager)
                .id(id)
                .name("test user")
                .nameUpdates(10)
                .insert(InsertMode.SYNC);
        assertEquals(10, user.nameUpdates.get());
        try (PreparedStatement preparedStatement = h2Connection.prepareStatement("SELECT \"name_updates\" FROM \"public\".\"user_metadata\" WHERE \"user_id\" = ?")) {
            preparedStatement.setObject(1, id);
            ResultSet rs = preparedStatement.executeQuery();
            assertTrue(rs.next());
            assertEquals(10, rs.getInt("name_updates"));
        }

        waitForDataPropagation();

        try (PreparedStatement preparedStatement = pgConnection.prepareStatement("SELECT \"name_updates\" FROM \"public\".\"user_metadata\" WHERE \"user_id\" = ?")) {
            preparedStatement.setObject(1, id);
            ResultSet rs = preparedStatement.executeQuery();
            assertTrue(rs.next());
            assertEquals(10, rs.getInt("name_updates"));
        }

        user.delete();
        assertTrue(user.isDeleted());

        try (PreparedStatement preparedStatement = h2Connection.prepareStatement("SELECT \"name_updates\" FROM \"public\".\"user_metadata\" WHERE \"user_id\" = ?")) {
            preparedStatement.setObject(1, id);
            ResultSet rs = preparedStatement.executeQuery();
            assertTrue(rs.next());
            assertEquals(10, rs.getInt("name_updates"));
        }

        waitForDataPropagation();

        try (PreparedStatement preparedStatement = pgConnection.prepareStatement("SELECT \"name_updates\" FROM \"public\".\"user_metadata\" WHERE \"user_id\" = ?")) {
            preparedStatement.setObject(1, id);
            ResultSet rs = preparedStatement.executeQuery();
            assertTrue(rs.next());
            assertEquals(10, rs.getInt("name_updates"));
        }
    }
}