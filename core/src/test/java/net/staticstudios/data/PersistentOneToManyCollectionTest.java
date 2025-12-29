package net.staticstudios.data;

import net.staticstudios.data.misc.DataTest;
import net.staticstudios.data.misc.TestUtils;
import net.staticstudios.data.mock.user.MockUser;
import net.staticstudios.data.mock.user.MockUserSession;
import net.staticstudios.utils.RandomUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

public class PersistentOneToManyCollectionTest extends DataTest {
    private static final int SESSION_COUNT = 20;

    private MockUser mockUser;
    private DataManager dataManager;

    @BeforeEach
    public void setUp() {
        dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(MockUser.class);
        dataManager.finishLoading();
        UUID id = UUID.randomUUID();
        mockUser = MockUser.builder(dataManager)
                .id(id)
                .name("test user")
                .insert(InsertMode.SYNC);
    }

    private List<MockUserSession> createSessions(int count) {
        List<MockUserSession> sessions = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            MockUserSession session = MockUserSession.builder(dataManager)
                    .id(UUID.randomUUID())
                    .timestamp(Timestamp.from(Instant.ofEpochSecond(RandomUtils.randomInt(0, 1_000_000_000))))
                    .insert(InsertMode.ASYNC);
            sessions.add(session);
        }
        return sessions;
    }

    @Test
    public void testAdd() {
        List<MockUserSession> sessions = createSessions(SESSION_COUNT);

        int size = mockUser.sessions.size();
        for (MockUserSession session : sessions) {
            assertNotEquals(mockUser.id.get(), session.userId.get());
            assertTrue(mockUser.sessions.add(session));
            assertEquals(mockUser.id.get(), session.userId.get());
            assertEquals(++size, mockUser.sessions.size());
        }

        for (MockUserSession session : sessions) {
            assertTrue(mockUser.sessions.contains(session));
        }

        waitForDataPropagation();

        Connection pgConnection = getConnection();

        try (PreparedStatement preparedStatement = pgConnection.prepareStatement("SELECT * FROM \"public\".\"user_sessions\" WHERE \"user_id\" = ?")) {
            preparedStatement.setObject(1, mockUser.id.get());
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                assertEquals(sessions.size(), TestUtils.getResultCount(resultSet));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testAddAll() {
        List<MockUserSession> sessions = createSessions(SESSION_COUNT);

        assertTrue(mockUser.sessions.addAll(sessions));
        assertEquals(SESSION_COUNT, mockUser.sessions.size());

        for (MockUserSession session : sessions) {
            assertTrue(mockUser.sessions.contains(session));
        }

        waitForDataPropagation();

        Connection pgConnection = getConnection();

        try (PreparedStatement preparedStatement = pgConnection.prepareStatement("SELECT * FROM \"public\".\"user_sessions\" WHERE \"user_id\" = ?")) {
            preparedStatement.setObject(1, mockUser.id.get());
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                assertEquals(sessions.size(), TestUtils.getResultCount(resultSet));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testRemove() {
        List<MockUserSession> sessions = createSessions(SESSION_COUNT);
        mockUser.sessions.addAll(sessions);

        for (MockUserSession session : sessions) {
            assertTrue(mockUser.sessions.contains(session));
        }

        waitForDataPropagation();

        Connection pgConnection = getConnection();

        try (PreparedStatement preparedStatement = pgConnection.prepareStatement("SELECT * FROM \"public\".\"user_sessions\" WHERE \"user_id\" = ?")) {
            preparedStatement.setObject(1, mockUser.id.get());
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                assertEquals(sessions.size(), TestUtils.getResultCount(resultSet));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        int size = mockUser.sessions.size();
        for (MockUserSession session : sessions) {
            assertTrue(mockUser.sessions.remove(session));
            assertEquals(--size, mockUser.sessions.size());
        }

        for (MockUserSession session : sessions) {
            assertFalse(mockUser.sessions.contains(session));
        }

        assertTrue(mockUser.sessions.isEmpty());

        waitForDataPropagation();

        try (PreparedStatement preparedStatement = pgConnection.prepareStatement("SELECT * FROM \"public\".\"user_sessions\" WHERE \"user_id\" = ?")) {
            preparedStatement.setObject(1, mockUser.id.get());
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                assertEquals(0, TestUtils.getResultCount(resultSet));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testRemoveAll() {
        List<MockUserSession> sessions = createSessions(SESSION_COUNT);
        mockUser.sessions.addAll(sessions);

        for (MockUserSession session : sessions) {
            assertTrue(mockUser.sessions.contains(session));
        }

        waitForDataPropagation();

        Connection pgConnection = getConnection();

        try (PreparedStatement preparedStatement = pgConnection.prepareStatement("SELECT * FROM \"public\".\"user_sessions\" WHERE \"user_id\" = ?")) {
            preparedStatement.setObject(1, mockUser.id.get());
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                assertEquals(sessions.size(), TestUtils.getResultCount(resultSet));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        assertTrue(mockUser.sessions.removeAll(sessions));
        assertEquals(0, mockUser.sessions.size());

        for (MockUserSession session : sessions) {
            assertFalse(mockUser.sessions.contains(session));
        }

        assertTrue(mockUser.sessions.isEmpty());

        waitForDataPropagation();

        try (PreparedStatement preparedStatement = pgConnection.prepareStatement("SELECT * FROM \"public\".\"user_sessions\" WHERE \"user_id\" = ?")) {
            preparedStatement.setObject(1, mockUser.id.get());
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                assertEquals(0, TestUtils.getResultCount(resultSet));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testContains() {
        List<MockUserSession> sessions = createSessions(SESSION_COUNT);
        for (MockUserSession session : sessions) {
            assertFalse(mockUser.sessions.contains(session));
            mockUser.sessions.add(session);
            assertTrue(mockUser.sessions.contains(session));
        }

        MockUserSession notAddedSession = createSessions(1).getFirst();
        assertFalse(mockUser.sessions.contains(notAddedSession));

        for (MockUserSession session : sessions) {
            mockUser.sessions.remove(session);
            assertFalse(mockUser.sessions.contains(session));
        }
    }

    @SuppressWarnings("SuspiciousMethodCalls")
    @Test
    public void testContainsAll() {
        assertTrue(mockUser.sessions.containsAll(new ArrayList<>()));
        assertFalse(mockUser.sessions.containsAll(List.of("not a session", "another non session")));
        List<MockUserSession> sessions = createSessions(SESSION_COUNT);
        mockUser.sessions.addAll(sessions);
        assertTrue(mockUser.sessions.containsAll(sessions));
        List<MockUserSession> nonAddedSessions = createSessions(3);
        List<MockUserSession> mixedSessions = new ArrayList<>(sessions.subList(0, SESSION_COUNT - 2));
        mixedSessions.addAll(nonAddedSessions);
        assertFalse(mockUser.sessions.containsAll(mixedSessions));
    }

    @Test
    public void testClear() {
        List<MockUserSession> sessions = createSessions(SESSION_COUNT);
        mockUser.sessions.addAll(sessions);
        assertEquals(SESSION_COUNT, mockUser.sessions.size());
        mockUser.sessions.clear();
        assertTrue(mockUser.sessions.isEmpty());
    }

    @Test
    public void testRetainAll() {
        List<MockUserSession> sessions = createSessions(SESSION_COUNT);
        mockUser.sessions.addAll(sessions);

        List<MockUserSession> toRetain = sessions.subList(0, SESSION_COUNT / 2);
        int nonAddedCount = 3;
        List<MockUserSession> junk = createSessions(nonAddedCount);
        toRetain.addAll(junk);
        assertTrue(mockUser.sessions.retainAll(toRetain));
        assertEquals(toRetain.size() - nonAddedCount, mockUser.sessions.size());
        for (int i = 0; i < toRetain.size() - nonAddedCount; i++) {
            assertTrue(mockUser.sessions.contains(sessions.get(i)));
        }
    }

    @Test
    public void testToArray() {
        List<MockUserSession> sessions = createSessions(SESSION_COUNT);
        mockUser.sessions.addAll(sessions);
        Object[] arr = mockUser.sessions.toArray();
        assertEquals(SESSION_COUNT, arr.length);
        for (Object o : arr) {
            assertTrue(sessions.contains(o));
        }

        MockUserSession[] typedArr = mockUser.sessions.toArray(new MockUserSession[0]);
        assertEquals(SESSION_COUNT, typedArr.length);
        for (MockUserSession s : typedArr) {
            assertTrue(sessions.contains(s));
        }
    }

    @Test
    public void testIterator() {
        List<MockUserSession> sessions = createSessions(SESSION_COUNT);
        mockUser.sessions.addAll(sessions);
        Iterator<MockUserSession> iterator = mockUser.sessions.iterator();
        int count = 0;
        while (iterator.hasNext()) {
            assertTrue(sessions.contains(iterator.next()));
            count++;
        }

        iterator = mockUser.sessions.iterator();
        count = 0;
        while (iterator.hasNext()) {
            MockUserSession session = iterator.next();
            assertTrue(sessions.contains(session));
            iterator.remove();
            assertFalse(mockUser.sessions.contains(session));
            count++;
        }

        assertEquals(SESSION_COUNT, count);
        assertTrue(mockUser.sessions.isEmpty());
    }

    @Test
    public void testEqualsAndHashCode() {
        MockUser anotherMockUser = MockUser.builder(dataManager)
                .id(UUID.randomUUID())
                .name("another user")
                .insert(InsertMode.SYNC);
        assertEquals(mockUser.sessions, mockUser.sessions);
        assertFalse(mockUser.sessions.equals(anotherMockUser.sessions));

    }

    @Test
    public void testAddHandlerUpdate() {
        MockUser user = MockUser.builder(dataManager)
                .id(UUID.randomUUID())
                .name("handler test user")
                .insert(InsertMode.SYNC);

        assertEquals(0, user.sessionAdditions.get());

        List<MockUserSession> sessions = createSessions(5);
        int i = 0;
        for (MockUserSession session : sessions) {
            user.sessions.add(session);
            waitForUpdateHandlers();

            assertEquals(++i, user.sessionAdditions.get());
        }
    }

    @Test
    public void testRemoveHandlerUpdate() {
        MockUser user = MockUser.builder(dataManager)
                .id(UUID.randomUUID())
                .name("handler test user")
                .insert(InsertMode.SYNC);

        List<MockUserSession> sessions = createSessions(5);
        user.sessions.addAll(sessions);
        waitForUpdateHandlers();

        assertEquals(5, user.sessions.size());
        assertEquals(0, user.sessionRemovals.get());

        int i = 0;
        for (MockUserSession session : sessions) {
            user.sessions.remove(session);
            waitForUpdateHandlers();

            assertEquals(++i, user.sessionRemovals.get());
        }
    }

    @Test
    public void testAddHandlerInsert() {
        MockUser user = MockUser.builder(dataManager)
                .id(UUID.randomUUID())
                .name("handler test user")
                .insert(InsertMode.SYNC);

        assertEquals(0, user.sessionAdditions.get());
        for (int i = 0; i < 5; i++) {
            MockUserSession.builder(dataManager)
                    .id(UUID.randomUUID())
                    .userId(user.id.get())
                    .timestamp(Timestamp.from(Instant.now()))
                    .insert(InsertMode.ASYNC);
            waitForUpdateHandlers();

            assertEquals(i + 1, user.sessionAdditions.get());
        }
    }

    @Test
    public void testRemoveHandlerDelete() {
        MockUser user = MockUser.builder(dataManager)
                .id(UUID.randomUUID())
                .name("handler test user")
                .insert(InsertMode.SYNC);

        List<MockUserSession> sessions = createSessions(5);
        user.sessions.addAll(sessions);
        waitForUpdateHandlers();

        assertEquals(5, user.sessions.size());
        assertEquals(0, user.sessionRemovals.get());

        int i = 0;
        for (MockUserSession session : sessions) {
            session.delete();
            waitForUpdateHandlers();

            assertEquals(++i, user.sessionRemovals.get());
        }
    }
}