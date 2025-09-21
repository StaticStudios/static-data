package net.staticstudios.data;

import net.staticstudios.data.insert.InsertContext;
import net.staticstudios.data.misc.DataTest;
import net.staticstudios.data.mock.MockUser;
import net.staticstudios.data.mock.MockUserFactory;
import net.staticstudios.data.mock.MockUserSettings;
import net.staticstudios.data.mock.MockUserSettingsFactory;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

public class ReferenceTest extends DataTest {
    @Test
    public void testCreateSettingsWithoutReference() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(MockUser.class);

        MockUserSettings settings = MockUserSettingsFactory.builder(dataManager)
                .id(UUID.randomUUID())
                .insert(InsertMode.SYNC);

        assertNotNull(settings);
    }

    @Test
    public void testCreateSettingsThenReference() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(MockUser.class);

        MockUserSettings settings = MockUserSettingsFactory.builder(dataManager)
                .id(UUID.randomUUID())
                .insert(InsertMode.SYNC);

        assertNotNull(settings);

        MockUser user = MockUserFactory.builder(dataManager)
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
        InsertContext ctx = dataManager.createInsertContext();
        MockUserSettingsFactory.builder(dataManager)
                .id(settingsId)
                .insert(ctx);

        MockUserFactory.builder(dataManager)
                .id(UUID.randomUUID())
                .name("test user")
                .settingsId(settingsId)
                .insert(ctx);

        ctx.insert(InsertMode.SYNC);
        MockUserSettings settings = ctx.get(MockUserSettings.class);
        MockUser user = ctx.get(MockUser.class);

        assertNotNull(settings);
        assertNotNull(user);
        assertSame(settings, user.settings.get());
    }

    @Test
    public void testChangeReference() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(MockUser.class);

        MockUserSettings settings = MockUserSettingsFactory.builder(dataManager)
                .id(UUID.randomUUID())
                .insert(InsertMode.SYNC);

        assertNotNull(settings);

        MockUser user = MockUserFactory.builder(dataManager)
                .id(UUID.randomUUID())
                .name("test user")
                .settingsId(settings.id.get())
                .insert(InsertMode.SYNC);

        assertNotNull(user);
        assertSame(settings, user.settings.get());

        MockUserSettings settings2 = MockUserSettingsFactory.builder(dataManager)
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
}