package net.staticstudios.data;

import net.staticstudios.data.misc.DataTest;
import net.staticstudios.data.mock.user.MockUser;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

public class SnapshotTest extends DataTest {

    @Test
    public void testPersistentValues() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(MockUser.class);

        UUID id = UUID.randomUUID();

        MockUser user = MockUser.builder(dataManager)
                .id(id)
                .name("some name")
                .age(0)
                .insert(InsertMode.ASYNC);

        MockUser snapshot = dataManager.createSnapshot(user);

        assertNotNull(snapshot);
        assertEquals(user.id.get(), snapshot.id.get());
        assertEquals(user.name.get(), snapshot.name.get());
        assertEquals(user.age.get(), snapshot.age.get());

        assertEquals(0, user.nameUpdates.get());
        assertEquals(0, snapshot.nameUpdates.get());

        user.name.set("new name");
        waitForUpdateHandlers();

        assertEquals("new name", user.name.get());
        assertEquals("some name", snapshot.name.get());
        assertEquals(1, user.nameUpdates.get());
        assertEquals(0, snapshot.nameUpdates.get());

        user.delete();

        assertTrue(user.isDeleted());
        assertFalse(snapshot.isDeleted());

        assertEquals(id, snapshot.id.get());
        assertEquals("some name", snapshot.name.get());
    }

    //todo: tests for cvs, refs, and all collection types
}