package net.staticstudios.data;

import net.staticstudios.data.insert.BatchInsert;
import net.staticstudios.data.misc.DataTest;
import net.staticstudios.data.mock.user.MockUser;
import net.staticstudios.data.mock.user.MockUserSettings;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

public class ReferenceTest extends DataTest {
    @Test
    public void testCreateSettingsWithoutReference() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(MockUser.class);

        MockUserSettings settings = MockUserSettings.builder(dataManager)
                .id(UUID.randomUUID())
                .insert(InsertMode.SYNC);

        assertNotNull(settings);
    }

    @Test
    public void testCreateSettingsThenReference() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(MockUser.class);

        MockUserSettings settings = MockUserSettings.builder(dataManager)
                .id(UUID.randomUUID())
                .insert(InsertMode.SYNC);

        assertNotNull(settings);

        MockUser user = MockUser.builder(dataManager)
                .id(UUID.randomUUID())
                .name("test user")
                .settingsId(settings.id.get())
                .insert(InsertMode.SYNC);

        assertNotNull(user);
        assertSame(settings, user.settings.get());
    }

    @Test
    public void testCreateUserAndReferenceInSingleInsert() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(MockUser.class);

        UUID settingsId = UUID.randomUUID();
        BatchInsert batch = dataManager.createBatchInsert();
        CompletableFuture<MockUserSettings> settingsCf = MockUserSettings.builder(dataManager)
                .id(settingsId)
                .insert(batch);

        CompletableFuture<MockUser> userCf = MockUser.builder(dataManager)
                .id(UUID.randomUUID())
                .name("test user")
                .settingsId(settingsId)
                .insert(batch);

        batch.insert(InsertMode.SYNC);
        MockUserSettings settings = settingsCf.join();
        MockUser user = userCf.join();

        assertNotNull(settings);
        assertNotNull(user);
        assertSame(settings, user.settings.get());
    }

    @Test
    public void testChangeReference() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(MockUser.class);

        MockUserSettings settings = MockUserSettings.builder(dataManager)
                .id(UUID.randomUUID())
                .insert(InsertMode.SYNC);

        assertNotNull(settings);

        MockUser user = MockUser.builder(dataManager)
                .id(UUID.randomUUID())
                .name("test user")
                .settingsId(settings.id.get())
                .insert(InsertMode.SYNC);

        assertNotNull(user);
        assertSame(settings, user.settings.get());

        MockUserSettings settings2 = MockUserSettings.builder(dataManager)
                .id(UUID.randomUUID())
                .insert(InsertMode.SYNC);

        assertNotNull(settings2);
        user.settingsId.set(settings2.id.get());
        assertSame(settings2, user.settings.get());

        user.settings.set(null);
        assertNull(user.settings.get());
        assertNull(user.settingsId.get());

        user.settings.set(settings);
        assertSame(settings, user.settings.get());
        assertEquals(settings.id.get(), user.settingsId.get());

        user.settings.set(settings2);
        assertSame(settings2, user.settings.get());
        assertEquals(settings2.id.get(), user.settingsId.get());
    }

    @Test
    public void testDeleteStrategyCascade() throws SQLException {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(MockUser.class);
        Connection h2Connection = getH2Connection(dataManager);
        Connection pgConnection = getConnection();
        UUID id = UUID.randomUUID();
        MockUserSettings settings = MockUserSettings.builder(dataManager)
                .id(id)
                .insert(InsertMode.SYNC);

        MockUser user = MockUser.builder(dataManager)
                .id(UUID.randomUUID())
                .name("test user")
                .settingsId(settings.id.get())
                .insert(InsertMode.SYNC);
        assertSame(settings, user.settings.get());

        try (PreparedStatement preparedStatement = h2Connection.prepareStatement("SELECT \"user_id\" FROM \"public\".\"user_settings\" WHERE \"user_id\" = ?")) {
            preparedStatement.setObject(1, id);
            ResultSet rs = preparedStatement.executeQuery();
            assertTrue(rs.next());
        }

        waitForDataPropagation();

        try (PreparedStatement preparedStatement = pgConnection.prepareStatement("SELECT user_id FROM public.user_settings WHERE user_id = ?")) {
            preparedStatement.setObject(1, id);
            ResultSet rs = preparedStatement.executeQuery();
            assertTrue(rs.next());
        }

        user.delete();
        assertTrue(user.isDeleted());
        assertTrue(settings.isDeleted());

        try (PreparedStatement preparedStatement = h2Connection.prepareStatement("SELECT \"user_id\" FROM \"public\".\"user_settings\" WHERE \"user_id\" = ?")) {
            preparedStatement.setObject(1, id);
            ResultSet rs = preparedStatement.executeQuery();
            assertFalse(rs.next());
        }

        waitForDataPropagation();

        try (PreparedStatement preparedStatement = pgConnection.prepareStatement("SELECT user_id FROM public.user_settings WHERE user_id = ?")) {
            preparedStatement.setObject(1, id);
            ResultSet rs = preparedStatement.executeQuery();
            assertFalse(rs.next());
        }
    }

    @Test
    public void testUpdateHandlerUpdate() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(MockUser.class);

        MockUser user = MockUser.builder(dataManager)
                .id(UUID.randomUUID())
                .name("name")
                .insert(InsertMode.SYNC);

        MockUserSettings settings = MockUserSettings.builder(dataManager)
                .id(UUID.randomUUID())
                .insert(InsertMode.SYNC);

        assertEquals(0, user.settingsUpdates.get());

        user.settings.set(settings);
        waitForUpdateHandlers();

        assertEquals(1, user.settingsUpdates.get());

        user.settings.set(settings);
        waitForUpdateHandlers();

        assertEquals(1, user.settingsUpdates.get());

        user.settings.set(null);
        waitForUpdateHandlers();

        assertEquals(2, user.settingsUpdates.get());

        user.settings.set(settings);
        waitForUpdateHandlers();

        assertEquals(3, user.settingsUpdates.get());

        MockUserSettings settings2 = MockUserSettings.builder(dataManager)
                .id(UUID.randomUUID())
                .insert(InsertMode.SYNC);

        user.settings.set(settings2);
        waitForUpdateHandlers();

        assertEquals(4, user.settingsUpdates.get());
    }

    @Test
    public void testReferenceNoFkey() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(MockUser.class);

        UUID bestBuddyId = UUID.randomUUID();

        MockUser user = MockUser.builder(dataManager)
                .id(UUID.randomUUID())
                .name("test user")
                .bestBuddyId(bestBuddyId)
                .insert(InsertMode.SYNC);

        assertNotNull(user);
        assertNull(user.bestBuddy.get());
        assertEquals(bestBuddyId, user.bestBuddyId.get());

        MockUser bestBuddy = MockUser.builder(dataManager)
                .id(bestBuddyId)
                .name("best buddy")
                .insert(InsertMode.SYNC);

        assertSame(bestBuddy, user.bestBuddy.get());
        bestBuddy.delete();

        assertNull(user.bestBuddy.get());
        assertEquals(bestBuddyId, user.bestBuddyId.get());
    }

}