package net.staticstudios.data;

import net.staticstudios.data.misc.DataTest;
import net.staticstudios.data.mock.user.MockUser;
import net.staticstudios.data.mock.user.MockUserFactory;
import net.staticstudios.data.mock.user.MockUserSession;
import net.staticstudios.data.mock.user.MockUserSessionFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

public class PersistentOneToManyCollectionTest extends DataTest {

    private MockUser mockUser;
    private DataManager dataManager;

    @BeforeEach
    public void setUp() {
        dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(MockUser.class);
        UUID id = UUID.randomUUID();
        mockUser = MockUserFactory.builder(dataManager)
                .id(id)
                .name("test user")
                .insert(InsertMode.SYNC);
    }

    @Test
    public void testEmpty() {
        assertTrue(mockUser.sessions.isEmpty());
    }

    @Test
    public void testAdd() {
        MockUserSession session = MockUserSessionFactory.builder(dataManager)
                .id(UUID.randomUUID())
                .timestamp(Timestamp.from(Instant.now()))
                .insert(InsertMode.SYNC);

        assertNull(session.userId.get());
        mockUser.sessions.add(session);
        assertEquals(mockUser.id.get(), session.userId.get());
        assertSame(mockUser, session.user.get());
        assertEquals(1, mockUser.sessions.size());
        assertSame(session, mockUser.sessions.iterator().next());
        assertEquals("[" + session + "]", mockUser.sessions.toString());
        //todo: validate the db
    }

    //todo: add more tests
    //todo: test other collection types
    //todo: retainall
    //todo: add/remove handlers
}