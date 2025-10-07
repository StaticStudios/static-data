package net.staticstudios.data;

import net.staticstudios.data.misc.DataTest;
import net.staticstudios.data.misc.TestUtils;
import net.staticstudios.data.mock.user.MockUser;
import net.staticstudios.data.mock.user.MockUserFactory;
import net.staticstudios.data.mock.user.MockUserSession;
import net.staticstudios.data.mock.user.MockUserSessionFactory;
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

        waitForDataPropagation();

        Connection pgConnection = getConnection();

        try (PreparedStatement preparedStatement = pgConnection.prepareStatement("SELECT * FROM public.user_sessions WHERE user_id = ?")) {
            preparedStatement.setObject(1, mockUser.id.get());
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                assertEquals(1, TestUtils.getResultCount(resultSet));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testRemove() {
        MockUserSession session = MockUserSessionFactory.builder(dataManager)
                .id(UUID.randomUUID())
                .timestamp(Timestamp.from(Instant.now()))
                .insert(InsertMode.SYNC);
        mockUser.sessions.add(session);
        waitForDataPropagation();
        assertTrue(mockUser.sessions.remove(session));
        assertFalse(mockUser.sessions.contains(session));
        assertEquals(0, mockUser.sessions.size());
        waitForDataPropagation();
        Connection pgConnection = getConnection();
        try (PreparedStatement preparedStatement = pgConnection.prepareStatement("SELECT * FROM public.user_sessions WHERE user_id = ?")) {
            preparedStatement.setObject(1, mockUser.id.get());
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                assertEquals(0, TestUtils.getResultCount(resultSet));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testClear() {
        MockUserSession session1 = MockUserSessionFactory.builder(dataManager)
                .id(UUID.randomUUID())
                .timestamp(Timestamp.from(Instant.now()))
                .insert(InsertMode.SYNC);
        MockUserSession session2 = MockUserSessionFactory.builder(dataManager)
                .id(UUID.randomUUID())
                .timestamp(Timestamp.from(Instant.now()))
                .insert(InsertMode.SYNC);
        mockUser.sessions.add(session1);
        mockUser.sessions.add(session2);
        waitForDataPropagation();
        mockUser.sessions.clear();
        assertTrue(mockUser.sessions.isEmpty());
        waitForDataPropagation();
        Connection pgConnection = getConnection();
        try (PreparedStatement preparedStatement = pgConnection.prepareStatement("SELECT * FROM public.user_sessions WHERE user_id = ?")) {
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
        MockUserSession session = MockUserSessionFactory.builder(dataManager)
                .id(UUID.randomUUID())
                .timestamp(Timestamp.from(Instant.now()))
                .insert(InsertMode.SYNC);
        assertFalse(mockUser.sessions.contains(session));
        mockUser.sessions.add(session);
        assertTrue(mockUser.sessions.contains(session));
        mockUser.sessions.remove(session);
        assertFalse(mockUser.sessions.contains(session));
    }

    @Test
    public void testSizeAndIsEmpty() {
        assertTrue(mockUser.sessions.isEmpty());
        assertEquals(0, mockUser.sessions.size());
        MockUserSession session = MockUserSessionFactory.builder(dataManager)
                .id(UUID.randomUUID())
                .timestamp(Timestamp.from(Instant.now()))
                .insert(InsertMode.SYNC);
        mockUser.sessions.add(session);
        assertFalse(mockUser.sessions.isEmpty());
        assertEquals(1, mockUser.sessions.size());
        mockUser.sessions.remove(session);
        assertTrue(mockUser.sessions.isEmpty());
        assertEquals(0, mockUser.sessions.size());
    }

    @Test
    public void testIterator() {
        List<MockUserSession> sessions = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            MockUserSession session = MockUserSessionFactory.builder(dataManager)
                    .id(UUID.randomUUID())
                    .timestamp(Timestamp.from(Instant.now()))
                    .insert(InsertMode.SYNC);
            mockUser.sessions.add(session);
            sessions.add(session);
        }
        Iterator<MockUserSession> iterator = mockUser.sessions.iterator();
        int count = 0;
        while (iterator.hasNext()) {
            assertTrue(sessions.contains(iterator.next()));
            count++;
        }
        assertEquals(3, count);
    }

    @Test
    public void testIteratorRemove() {
        for (int i = 0; i < 3; i++) {
            MockUserSession session = MockUserSessionFactory.builder(dataManager)
                    .id(UUID.randomUUID())
                    .timestamp(Timestamp.from(Instant.now()))
                    .insert(InsertMode.SYNC);
            mockUser.sessions.add(session);
        }
        Iterator<MockUserSession> iterator = mockUser.sessions.iterator();
        MockUserSession toRemove = iterator.next();
        iterator.remove();
        assertFalse(mockUser.sessions.contains(toRemove));
        assertEquals(2, mockUser.sessions.size());
        waitForDataPropagation();
        Connection pgConnection = getConnection();
        try (PreparedStatement preparedStatement = pgConnection.prepareStatement("SELECT * FROM public.user_sessions WHERE user_id = ?")) {
            preparedStatement.setObject(1, mockUser.id.get());
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                assertEquals(2, TestUtils.getResultCount(resultSet));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testToArray() {
        List<MockUserSession> sessions = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            MockUserSession session = MockUserSessionFactory.builder(dataManager)
                    .id(UUID.randomUUID())
                    .timestamp(Timestamp.from(Instant.now()))
                    .insert(InsertMode.SYNC);
            mockUser.sessions.add(session);
            sessions.add(session);
        }
        Object[] arr = mockUser.sessions.toArray();
        assertEquals(2, arr.length);
        for (Object o : arr) {
            assertTrue(sessions.contains(o));
        }
        MockUserSession[] typedArr = mockUser.sessions.toArray(new MockUserSession[0]);
        assertEquals(2, typedArr.length);
        for (MockUserSession s : typedArr) {
            assertTrue(sessions.contains(s));
        }
    }

    @Test
    public void testToString() {
        mockUser.sessions.clear();
        MockUserSession session = MockUserSessionFactory.builder(dataManager)
                .id(UUID.randomUUID())
                .timestamp(Timestamp.from(Instant.now()))
                .insert(InsertMode.SYNC);
        mockUser.sessions.add(session);
        String expected = String.format("[%s]", session);
        assertEquals(expected, mockUser.sessions.toString());
    }

    @Test
    public void testAddAll() {
        List<MockUserSession> sessions = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            MockUserSession session = MockUserSessionFactory.builder(dataManager)
                    .id(UUID.randomUUID())
                    .timestamp(Timestamp.from(Instant.now()))
                    .insert(InsertMode.SYNC);
            sessions.add(session);
        }
        assertTrue(mockUser.sessions.addAll(sessions));
        assertEquals(3, mockUser.sessions.size());
        for (MockUserSession session : sessions) {
            assertTrue(mockUser.sessions.contains(session));
        }
        waitForDataPropagation();
        Connection pgConnection = getConnection();
        try (PreparedStatement preparedStatement = pgConnection.prepareStatement("SELECT * FROM public.user_sessions WHERE user_id = ?")) {
            preparedStatement.setObject(1, mockUser.id.get());
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                assertEquals(3, TestUtils.getResultCount(resultSet));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testRemoveAll() {
        List<MockUserSession> sessions = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            MockUserSession session = MockUserSessionFactory.builder(dataManager)
                    .id(UUID.randomUUID())
                    .timestamp(Timestamp.from(Instant.now()))
                    .insert(InsertMode.SYNC);
            mockUser.sessions.add(session);
            sessions.add(session);
        }
        waitForDataPropagation();
        assertTrue(mockUser.sessions.removeAll(sessions));
        assertEquals(0, mockUser.sessions.size());
        for (MockUserSession session : sessions) {
            assertFalse(mockUser.sessions.contains(session));
        }
        waitForDataPropagation();
        Connection pgConnection = getConnection();
        try (PreparedStatement preparedStatement = pgConnection.prepareStatement("SELECT * FROM public.user_sessions WHERE user_id = ?")) {
            preparedStatement.setObject(1, mockUser.id.get());
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                assertEquals(0, TestUtils.getResultCount(resultSet));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testRetainAll() {
        List<MockUserSession> sessions = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            MockUserSession session = MockUserSessionFactory.builder(dataManager)
                    .id(UUID.randomUUID())
                    .timestamp(Timestamp.from(Instant.now()))
                    .insert(InsertMode.SYNC);
            mockUser.sessions.add(session);
            sessions.add(session);
        }
        List<MockUserSession> retain = List.of(sessions.get(0));
        assertTrue(mockUser.sessions.retainAll(retain));
        assertEquals(1, mockUser.sessions.size());
        assertTrue(mockUser.sessions.contains(sessions.get(0)));
        assertFalse(mockUser.sessions.contains(sessions.get(1)));
        assertFalse(mockUser.sessions.contains(sessions.get(2)));
        waitForDataPropagation();
        Connection pgConnection = getConnection();
        try (PreparedStatement preparedStatement = pgConnection.prepareStatement("SELECT * FROM public.user_sessions WHERE user_id = ?")) {
            preparedStatement.setObject(1, mockUser.id.get());
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                assertEquals(1, TestUtils.getResultCount(resultSet));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testToArrayTyped() {
        List<MockUserSession> sessions = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            MockUserSession session = MockUserSessionFactory.builder(dataManager)
                    .id(UUID.randomUUID())
                    .timestamp(Timestamp.from(Instant.now()))
                    .insert(InsertMode.SYNC);
            mockUser.sessions.add(session);
            sessions.add(session);
        }
        MockUserSession[] arr = new MockUserSession[2];
        MockUserSession[] result = mockUser.sessions.toArray(arr);
        assertEquals(2, result.length);
        for (MockUserSession s : result) {
            assertTrue(sessions.contains(s));
        }
    }

    //todo: test other collection types
    //todo: add/remove handlers
}