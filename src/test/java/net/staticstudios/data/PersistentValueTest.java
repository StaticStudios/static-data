package net.staticstudios.data;

import net.staticstudios.data.misc.DataTest;
import net.staticstudios.data.misc.MockEnvironment;
import net.staticstudios.data.mock.MockUser;
import net.staticstudios.data.mock.MockUserFactory;
import net.staticstudios.data.mock.MockUserSettings;
import net.staticstudios.data.util.ColumnValuePair;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

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
            MockUser user = dataManager.get(MockUser.class, ColumnValuePair.of("id", id));
            assertEquals("user " + id, user.name.get());
            assertNull(user.age.get());
        }

        waitForDataPropagation();

        MockEnvironment environment2 = createMockEnvironment();
        DataManager dataManager2 = environment2.dataManager();
        dataManager2.load(MockUser.class);
        for (UUID id : userIds) {
            MockUser user = dataManager2.get(MockUser.class, ColumnValuePair.of("id", id));
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
                .insert(InsertMode.SYNC);
        assertEquals("test user", mockUser.name.get());
        mockUser.name.set("updated name");
        assertEquals("updated name", mockUser.name.get());

        assertNull(mockUser.age.get());
        mockUser.age.set(25);
        assertEquals(25, mockUser.age.get());

        mockUser = dataManager.get(MockUser.class, ColumnValuePair.of("id", id));
        mockUser = null; // remove strong reference
        System.gc();
        mockUser = dataManager.get(MockUser.class, ColumnValuePair.of("id", id)); // should have a cache miss

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

        MockUserSettings settings = MockUserSettings.create(dataManager, UUID.randomUUID());
        assertNull(mockUser.settings.get());
        mockUser.settings.set(settings);
        assertEquals(settings, mockUser.settings.get());
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
                .insert(InsertMode.SYNC);
        assertEquals("test user", mockUser.name.get());
        //first instance was created, handler should be registered
        assertEquals(1, dataManager.getUpdateHandlers("public", "users", "name", MockUser.class).size());
        mockUser = null; // remove strong reference
        System.gc();
        mockUser = dataManager.get(MockUser.class, ColumnValuePair.of("id", id)); // should have a cache miss
        //the handler for this pv should not have been registered again
        assertEquals(1, dataManager.getUpdateHandlers("public", "users", "name", MockUser.class).size());
        assertEquals(0, mockUser.getNameUpdates());

        mockUser.name.set("new name");
        assertEquals(1, mockUser.getNameUpdates());
        mockUser.name.set("new name");
        assertEquals(1, mockUser.getNameUpdates());
        mockUser.name.set("new name2");
        assertEquals(2, mockUser.getNameUpdates());
        mockUser.name.set("new name");
        assertEquals(3, mockUser.getNameUpdates());
    }
}