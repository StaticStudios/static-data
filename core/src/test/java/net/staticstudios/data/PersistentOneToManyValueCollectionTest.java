package net.staticstudios.data;

import net.staticstudios.data.misc.DataTest;
import net.staticstudios.data.misc.TestUtils;
import net.staticstudios.data.mock.user.MockUser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

public class PersistentOneToManyValueCollectionTest extends DataTest {
    private static final int NUMBER_COUNT = 20;

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

    private List<Integer> createNumbers(int count) {
        return createNumbersStartingAt(0, count);
    }

    private List<Integer> createNumbersStartingAt(int start, int count) {
        List<Integer> numbers = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            numbers.add(start + i);
        }
        return numbers;
    }

    @Test
    public void testAdd() {
        List<Integer> numbers = createNumbers(NUMBER_COUNT);

        int size = mockUser.favoriteNumbers.size();
        for (Integer number : numbers) {
            assertTrue(mockUser.favoriteNumbers.add(number));
            assertEquals(++size, mockUser.favoriteNumbers.size());
        }

        for (Integer number : numbers) {
            assertTrue(mockUser.favoriteNumbers.contains(number));
        }

        waitForDataPropagation();

        Connection pgConnection = getConnection();

        try (PreparedStatement preparedStatement = pgConnection.prepareStatement("SELECT * FROM \"public\".\"favorite_numbers\" WHERE \"user_id\" = ?")) {
            preparedStatement.setObject(1, mockUser.id.get());
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                assertEquals(numbers.size(), TestUtils.getResultCount(resultSet));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testAddAll() {
        List<Integer> numbers = createNumbers(NUMBER_COUNT);

        assertTrue(mockUser.favoriteNumbers.addAll(numbers));
        assertEquals(NUMBER_COUNT, mockUser.favoriteNumbers.size());

        for (Integer number : numbers) {
            assertTrue(mockUser.favoriteNumbers.contains(number));
        }

        waitForDataPropagation();

        Connection pgConnection = getConnection();

        try (PreparedStatement preparedStatement = pgConnection.prepareStatement("SELECT * FROM \"public\".\"favorite_numbers\" WHERE \"user_id\" = ?")) {
            preparedStatement.setObject(1, mockUser.id.get());
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                assertEquals(numbers.size(), TestUtils.getResultCount(resultSet));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testRemove() {
        List<Integer> numbers = createNumbers(NUMBER_COUNT);
        mockUser.favoriteNumbers.addAll(numbers);

        for (Integer number : numbers) {
            assertTrue(mockUser.favoriteNumbers.contains(number));
        }

        waitForDataPropagation();

        Connection pgConnection = getConnection();

        try (PreparedStatement preparedStatement = pgConnection.prepareStatement("SELECT * FROM \"public\".\"favorite_numbers\" WHERE \"user_id\" = ?")) {
            preparedStatement.setObject(1, mockUser.id.get());
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                assertEquals(numbers.size(), TestUtils.getResultCount(resultSet));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        int size = mockUser.favoriteNumbers.size();
        for (Integer number : numbers) {
            assertTrue(mockUser.favoriteNumbers.remove(number));
            assertEquals(--size, mockUser.favoriteNumbers.size());
        }

        for (Integer number : numbers) {
            assertFalse(mockUser.favoriteNumbers.contains(number));
        }

        assertTrue(mockUser.favoriteNumbers.isEmpty());

        waitForDataPropagation();

        try (PreparedStatement preparedStatement = pgConnection.prepareStatement("SELECT * FROM \"public\".\"favorite_numbers\" WHERE \"user_id\" = ?")) {
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
        List<Integer> numbers = createNumbers(NUMBER_COUNT);
        mockUser.favoriteNumbers.addAll(numbers);

        for (Integer number : numbers) {
            assertTrue(mockUser.favoriteNumbers.contains(number));
        }

        waitForDataPropagation();

        Connection pgConnection = getConnection();

        try (PreparedStatement preparedStatement = pgConnection.prepareStatement("SELECT * FROM \"public\".\"favorite_numbers\" WHERE \"user_id\" = ?")) {
            preparedStatement.setObject(1, mockUser.id.get());
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                assertEquals(numbers.size(), TestUtils.getResultCount(resultSet));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        assertTrue(mockUser.favoriteNumbers.removeAll(numbers));
        assertEquals(0, mockUser.favoriteNumbers.size());

        for (Integer number : numbers) {
            assertFalse(mockUser.favoriteNumbers.contains(number));
        }

        assertTrue(mockUser.favoriteNumbers.isEmpty());

        waitForDataPropagation();

        try (PreparedStatement preparedStatement = pgConnection.prepareStatement("SELECT * FROM \"public\".\"favorite_numbers\" WHERE \"user_id\" = ?")) {
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
        List<Integer> numbers = createNumbersStartingAt(0, NUMBER_COUNT);
        for (Integer number : numbers) {
            assertFalse(mockUser.favoriteNumbers.contains(number));
            mockUser.favoriteNumbers.add(number);
            assertTrue(mockUser.favoriteNumbers.contains(number));
        }

        Integer notAddedNumber = createNumbersStartingAt(NUMBER_COUNT, 1).get(0);
        assertFalse(mockUser.favoriteNumbers.contains(notAddedNumber));

        for (Integer number : numbers) {
            mockUser.favoriteNumbers.remove(number);
            assertFalse(mockUser.favoriteNumbers.contains(number));
        }
    }

    @SuppressWarnings("SuspiciousMethodCalls")
    @Test
    public void testContainsAll() {
        assertTrue(mockUser.favoriteNumbers.containsAll(new ArrayList<>()));
        assertFalse(mockUser.favoriteNumbers.containsAll(List.of("not a number", "another non number")));
        List<Integer> numbers = createNumbersStartingAt(0, NUMBER_COUNT);
        mockUser.favoriteNumbers.addAll(numbers);
        assertTrue(mockUser.favoriteNumbers.containsAll(numbers));
        List<Integer> nonAddedNumbers = createNumbersStartingAt(NUMBER_COUNT, 3);
        List<Integer> mixedNumbers = new ArrayList<>(numbers.subList(0, NUMBER_COUNT - 2));
        mixedNumbers.addAll(nonAddedNumbers);
        assertFalse(mockUser.favoriteNumbers.containsAll(mixedNumbers));
    }

    @Test
    public void testClear() {
        List<Integer> numbers = createNumbers(NUMBER_COUNT);
        mockUser.favoriteNumbers.addAll(numbers);
        assertEquals(NUMBER_COUNT, mockUser.favoriteNumbers.size());
        mockUser.favoriteNumbers.clear();
        assertTrue(mockUser.favoriteNumbers.isEmpty());
    }

    @Test
    public void testRetainAll() {
        List<Integer> numbers = createNumbersStartingAt(0, NUMBER_COUNT);
        mockUser.favoriteNumbers.addAll(numbers);

        List<Integer> toRetain = new ArrayList<>(numbers.subList(0, NUMBER_COUNT / 2));
        int nonAddedCount = 3;
        List<Integer> junk = createNumbersStartingAt(NUMBER_COUNT, nonAddedCount);
        toRetain.addAll(junk);
        assertTrue(mockUser.favoriteNumbers.retainAll(toRetain));
        assertEquals(toRetain.size() - nonAddedCount, mockUser.favoriteNumbers.size());
        for (int i = 0; i < toRetain.size() - nonAddedCount; i++) {
            assertTrue(mockUser.favoriteNumbers.contains(numbers.get(i)));
        }
    }

    @Test
    public void testToArray() {
        List<Integer> numbers = createNumbers(NUMBER_COUNT);
        mockUser.favoriteNumbers.addAll(numbers);
        Object[] arr = mockUser.favoriteNumbers.toArray();
        assertEquals(NUMBER_COUNT, arr.length);
        for (Object o : arr) {
            assertTrue(numbers.contains(o));
        }

        Integer[] typedArr = mockUser.favoriteNumbers.toArray(new Integer[0]);
        assertEquals(NUMBER_COUNT, typedArr.length);
        for (Integer n : typedArr) {
            assertTrue(numbers.contains(n));
        }
    }

    @Test
    public void testIterator() {
        List<Integer> numbers = createNumbers(NUMBER_COUNT);
        mockUser.favoriteNumbers.addAll(numbers);
        java.util.Iterator<Integer> iterator = mockUser.favoriteNumbers.iterator();
        int count = 0;
        while (iterator.hasNext()) {
            assertTrue(numbers.contains(iterator.next()));
            count++;
        }

        iterator = mockUser.favoriteNumbers.iterator();
        count = 0;
        while (iterator.hasNext()) {
            Integer number = iterator.next();
            assertTrue(numbers.contains(number));
            iterator.remove();
            assertFalse(mockUser.favoriteNumbers.contains(number));
            count++;
        }

        assertEquals(NUMBER_COUNT, count);
        assertTrue(mockUser.favoriteNumbers.isEmpty());
    }

    @Test
    public void testEqualsAndHashCode() {
        MockUser anotherMockUser = MockUser.builder(dataManager)
                .id(UUID.randomUUID())
                .name("another user")
                .insert(InsertMode.SYNC);
        assertTrue(mockUser.favoriteNumbers.equals(mockUser.favoriteNumbers));
        assertFalse(mockUser.favoriteNumbers.equals(anotherMockUser.favoriteNumbers));
    }

    @Test
    public void testAddHandlerUpdate() {
        MockUser user = MockUser.builder(dataManager)
                .id(UUID.randomUUID())
                .name("handler test user")
                .insert(InsertMode.SYNC);

        MockUser user2 = MockUser.builder(dataManager)
                .id(UUID.randomUUID())
                .name("handler test user")
                .insert(InsertMode.SYNC);

        assertEquals(0, user.favoriteNumberAdditions.get());

        Connection pgConnection = getConnection();
        List<Integer> numbers = createNumbers(5);
        List<UUID> ids = new ArrayList<>();
        for (Integer number : numbers) {
            ids.add(UUID.randomUUID());
        }
        int i = 0;
        for (Integer number : numbers) {
            try (PreparedStatement preparedStatement = pgConnection.prepareStatement(
                    "INSERT INTO \"public\".\"favorite_numbers\" (\"favorite_numbers_id\", \"user_id\", \"number\") VALUES (?, ?, ?)"
            )) {
                preparedStatement.setObject(1, ids.get(i));
                preparedStatement.setObject(2, user2.id.get());
                preparedStatement.setInt(3, number);
                preparedStatement.executeUpdate();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            try (PreparedStatement preparedStatement = pgConnection.prepareStatement(
                    "UPDATE \"public\".\"favorite_numbers\" SET \"user_id\" = ? WHERE \"favorite_numbers_id\" = ?"
            )) {
                preparedStatement.setObject(1, user.id.get());
                preparedStatement.setObject(2, ids.get(i));
                preparedStatement.executeUpdate();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            waitForDataPropagation();

            assertEquals(++i, user.favoriteNumberAdditions.get());
        }
    }

    @Test
    public void testRemoveHandlerUpdate() {
        MockUser user = MockUser.builder(dataManager)
                .id(UUID.randomUUID())
                .name("handler test user")
                .insert(InsertMode.SYNC);
        MockUser user2 = MockUser.builder(dataManager)
                .id(UUID.randomUUID())
                .name("handler test user")
                .insert(InsertMode.SYNC);

        Connection pgConnection = getConnection();
        List<Integer> numbers = createNumbers(5);
        List<UUID> ids = new ArrayList<>();
        for (Integer number : numbers) {
            ids.add(UUID.randomUUID());
        }
        int i = 0;
        for (Integer number : numbers) {
            try (PreparedStatement preparedStatement = pgConnection.prepareStatement(
                    "INSERT INTO \"public\".\"favorite_numbers\" (\"favorite_numbers_id\", \"user_id\", \"number\") VALUES (?, ?, ?)"
            )) {
                preparedStatement.setObject(1, ids.get(i));
                preparedStatement.setObject(2, user.id.get());
                preparedStatement.setInt(3, number);
                preparedStatement.executeUpdate();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            try (PreparedStatement preparedStatement = pgConnection.prepareStatement(
                    "UPDATE \"public\".\"favorite_numbers\" SET \"user_id\" = ? WHERE \"favorite_numbers_id\" = ?"
            )) {
                preparedStatement.setObject(1, user2.id.get());
                preparedStatement.setObject(2, ids.get(i));
                preparedStatement.executeUpdate();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            waitForDataPropagation();

            assertEquals(++i, user.favoriteNumberRemovals.get());
        }
    }

    @Test
    public void testAddHandlerInsert() {
        MockUser user = MockUser.builder(dataManager)
                .id(UUID.randomUUID())
                .name("handler test user")
                .insert(InsertMode.SYNC);

        assertEquals(0, user.favoriteNumberAdditions.get());

        List<Integer> numbers = createNumbers(5);
        int i = 0;
        for (Integer number : numbers) {
            user.favoriteNumbers.add(number);
            assertEquals(++i, user.favoriteNumberAdditions.get());
        }
    }

    @Test
    public void testRemoveHandlerDelete() {
        MockUser user = MockUser.builder(dataManager)
                .id(UUID.randomUUID())
                .name("handler test user")
                .insert(InsertMode.SYNC);

        List<Integer> numbers = createNumbers(5);
        user.favoriteNumbers.addAll(numbers);
        waitForDataPropagation();

        assertEquals(5, user.favoriteNumbers.size());
        assertEquals(0, user.favoriteNumberRemovals.get());

        int i = 0;
        for (Integer number : numbers) {
            user.favoriteNumbers.remove(number);

            assertEquals(++i, user.favoriteNumberRemovals.get());
        }
    }
}