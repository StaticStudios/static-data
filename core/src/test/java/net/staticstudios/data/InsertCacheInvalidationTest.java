package net.staticstudios.data;

import net.staticstudios.data.misc.DataTest;
import net.staticstudios.data.mock.user.MockUser;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

public class InsertCacheInvalidationTest extends DataTest {

    @Test
    public void testInsertAfterDeleteInvalidatesCache() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(MockUser.class);
        dataManager.finishLoading();

        UUID id = UUID.randomUUID();
        String name = "cache test user";

        MockUser inserted = MockUser.builder(dataManager)
                .id(id)
                .name(name)
                .insert(InsertMode.SYNC);

        MockUser queried = MockUser.query(dataManager).where(w -> w.idIs(id)).findOne();
        assertNotNull(queried);
        assertSame(inserted, queried);

        inserted.delete();

        MockUser queriedAfterDelete = MockUser.query(dataManager).where(w -> w.idIs(id)).findOne();
        assertNull(queriedAfterDelete);

        MockUser reinserted = MockUser.builder(dataManager)
                .id(id)
                .name(name)
                .insert(InsertMode.SYNC);

        MockUser queriedAfterReinsert = MockUser.query(dataManager).where(w -> w.idIs(id)).findOne();
        assertNotNull(queriedAfterReinsert, "Query after re-insert should not return null; cache was not invalidated by the insert");
        assertSame(reinserted, queriedAfterReinsert);
    }
}

