package net.staticstudios.data;

import net.staticstudios.data.misc.DataTest;
import net.staticstudios.data.misc.TestUtils;
import net.staticstudios.data.mock.user.MockUser;
import net.staticstudios.data.mock.user.MockUserFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

public class PersistentManyToManyCollectionTest extends DataTest {
    private static final int FRIEND_COUNT = 20;

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

    private List<MockUser> createFriends(int count) {
        List<MockUser> friends = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            MockUser friend = MockUserFactory.builder(dataManager)
                    .id(UUID.randomUUID())
                    .name("friend " + i)
                    .insert(InsertMode.ASYNC);
            friends.add(friend);
        }
        return friends;
    }

    @Test
    public void testAdd() {
        List<MockUser> friends = createFriends(FRIEND_COUNT);

        int size = mockUser.friends.size();
        for (MockUser friend : friends) {
            assertTrue(mockUser.friends.add(friend));
            assertEquals(++size, mockUser.friends.size());
        }

        for (MockUser friend : friends) {
            assertTrue(mockUser.friends.contains(friend));
        }

        waitForDataPropagation();

        Connection pgConnection = getConnection();

        try (PreparedStatement preparedStatement = pgConnection.prepareStatement("SELECT * FROM \"public\".\"user_friends\" WHERE \"users_id\" = ?")) {
            preparedStatement.setObject(1, mockUser.id.get());
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                assertEquals(friends.size(), TestUtils.getResultCount(resultSet));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testAddAll() {
        List<MockUser> friends = createFriends(FRIEND_COUNT);

        assertTrue(mockUser.friends.addAll(friends));
        assertEquals(friends.size(), mockUser.friends.size());

        for (MockUser friend : friends) {
            assertTrue(mockUser.friends.contains(friend));
        }

        waitForDataPropagation();

        Connection pgConnection = getConnection();

        try (PreparedStatement preparedStatement = pgConnection.prepareStatement("SELECT * FROM \"public\".\"user_friends\" WHERE \"users_id\" = ?")) {
            preparedStatement.setObject(1, mockUser.id.get());
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                assertEquals(friends.size(), TestUtils.getResultCount(resultSet));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testRemove() {
        List<MockUser> friends = createFriends(FRIEND_COUNT);
        mockUser.friends.addAll(friends);

        for (MockUser friend : friends) {
            assertTrue(mockUser.friends.contains(friend));
        }

        waitForDataPropagation();

        Connection pgConnection = getConnection();

        try (PreparedStatement preparedStatement = pgConnection.prepareStatement("SELECT * FROM \"public\".\"user_friends\" WHERE \"users_id\" = ?")) {
            preparedStatement.setObject(1, mockUser.id.get());
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                assertEquals(friends.size(), TestUtils.getResultCount(resultSet));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        int size = mockUser.friends.size();
        for (MockUser friend : friends) {
            assertTrue(mockUser.friends.remove(friend));
            assertEquals(--size, mockUser.friends.size());
        }

        for (MockUser friend : friends) {
            assertFalse(mockUser.friends.contains(friend));
        }

        assertTrue(mockUser.friends.isEmpty());

        waitForDataPropagation();

        try (PreparedStatement preparedStatement = pgConnection.prepareStatement("SELECT * FROM \"public\".\"user_friends\" WHERE \"users_id\" = ?")) {
            preparedStatement.setObject(1, mockUser.id.get());
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                assertEquals(0, TestUtils.getResultCount(resultSet));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("SuspiciousMethodCalls")
    @Test
    public void testRemoveAll() {
        assertFalse(mockUser.friends.removeAll(new ArrayList<>()));
        assertFalse(mockUser.friends.removeAll(List.of("not a user")));

        List<MockUser> friends = createFriends(FRIEND_COUNT);
        mockUser.friends.addAll(friends);

        for (MockUser friend : friends) {
            assertTrue(mockUser.friends.contains(friend));
        }

        waitForDataPropagation();

        Connection pgConnection = getConnection();

        try (PreparedStatement preparedStatement = pgConnection.prepareStatement("SELECT * FROM \"public\".\"user_friends\" WHERE \"users_id\" = ?")) {
            preparedStatement.setObject(1, mockUser.id.get());
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                assertEquals(friends.size(), TestUtils.getResultCount(resultSet));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        assertTrue(mockUser.friends.removeAll(friends));
        assertEquals(0, mockUser.friends.size());

        for (MockUser friend : friends) {
            assertFalse(mockUser.friends.contains(friend));
        }

        assertTrue(mockUser.friends.isEmpty());

        waitForDataPropagation();

        try (PreparedStatement preparedStatement = pgConnection.prepareStatement("SELECT * FROM \"public\".\"user_friends\" WHERE \"users_id\" = ?")) {
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
        List<MockUser> friends = createFriends(FRIEND_COUNT);
        for (MockUser friend : friends) {
            assertFalse(mockUser.friends.contains(friend));
            mockUser.friends.add(friend);
            assertTrue(mockUser.friends.contains(friend));
        }

        MockUser nonFriend = createFriends(1).getFirst();
        assertFalse(mockUser.friends.contains(nonFriend));

        for (MockUser friend : friends) {
            mockUser.friends.remove(friend);
            assertFalse(mockUser.friends.contains(friend));
        }
    }

    @SuppressWarnings("SuspiciousMethodCalls")
    @Test
    public void testContainsAll() {
        assertTrue(mockUser.friends.containsAll(new ArrayList<>()));
        assertFalse(mockUser.friends.containsAll(List.of("not a user", "another non user")));
        List<MockUser> friends = createFriends(FRIEND_COUNT);
        mockUser.friends.addAll(friends);
        assertTrue(mockUser.friends.containsAll(friends));
        List<MockUser> nonFriends = createFriends(3);
        List<MockUser> mixed = new ArrayList<>(friends.subList(0, FRIEND_COUNT - 2));
        mixed.addAll(nonFriends);
        assertFalse(mockUser.friends.containsAll(mixed));
    }

    @Test
    public void testClear() {
        List<MockUser> friends = createFriends(FRIEND_COUNT);
        mockUser.friends.addAll(friends);
        assertEquals(FRIEND_COUNT, mockUser.friends.size());
        mockUser.friends.clear();
        assertTrue(mockUser.friends.isEmpty());
    }

    @Test
    public void testRetainAll() {
        List<MockUser> friends = createFriends(FRIEND_COUNT);
        mockUser.friends.addAll(friends);

        List<MockUser> toRetain = friends.subList(0, FRIEND_COUNT / 2);
        int nonFriends = 3;
        List<MockUser> junk = createFriends(nonFriends);
        toRetain.addAll(junk);
        assertTrue(mockUser.friends.retainAll(toRetain));
        assertEquals(toRetain.size() - nonFriends, mockUser.friends.size());
        for (int i = 0; i < toRetain.size() - nonFriends; i++) {
            assertTrue(mockUser.friends.contains(friends.get(i)));
        }
    }

    @Test
    public void testToArray() {
        List<MockUser> friends = createFriends(FRIEND_COUNT);
        mockUser.friends.addAll(friends);
        Object[] arr = mockUser.friends.toArray();
        assertEquals(FRIEND_COUNT, arr.length);
        for (Object o : arr) {
            assertTrue(friends.contains(o));
        }

        MockUser[] typedArr = mockUser.friends.toArray(new MockUser[0]);
        assertEquals(FRIEND_COUNT, typedArr.length);
        for (MockUser u : typedArr) {
            assertTrue(friends.contains(u));
        }
    }

    @Test
    public void testIterator() {
        List<MockUser> friends = createFriends(FRIEND_COUNT);
        mockUser.friends.addAll(friends);
        Iterator<MockUser> iterator = mockUser.friends.iterator();
        int count = 0;
        while (iterator.hasNext()) {
            assertTrue(friends.contains(iterator.next()));
            count++;
        }

        iterator = mockUser.friends.iterator();
        count = 0;
        while (iterator.hasNext()) {
            MockUser friend = iterator.next();
            assertTrue(friends.contains(friend));
            iterator.remove();
            assertFalse(mockUser.friends.contains(friend));
            count++;
        }

        assertEquals(FRIEND_COUNT, count);
        assertTrue(mockUser.friends.isEmpty());
    }

    @Test
    public void testEqualsAndHashCode() {
        MockUser anotherUser = MockUserFactory.builder(dataManager)
                .id(UUID.randomUUID())
                .name("another user")
                .insert(InsertMode.SYNC);

        assertEquals(mockUser.friends, anotherUser.friends);
        assertEquals(mockUser.friends.hashCode(), anotherUser.friends.hashCode());

        List<MockUser> friends = createFriends(FRIEND_COUNT);
        mockUser.friends.addAll(friends);
        assertNotEquals(mockUser.friends, anotherUser.friends);
        assertNotEquals(mockUser.friends.hashCode(), anotherUser.friends.hashCode());
        anotherUser.friends.addAll(friends);
        assertEquals(mockUser.friends, anotherUser.friends);
        assertEquals(mockUser.friends.hashCode(), anotherUser.friends.hashCode());
    }
}