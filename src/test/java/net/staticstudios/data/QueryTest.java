package net.staticstudios.data;

import net.staticstudios.data.misc.DataTest;
import net.staticstudios.data.mock.MockUser;
import net.staticstudios.data.mock.MockUserFactory;
import net.staticstudios.data.mock.MockUserQuery;
import net.staticstudios.data.query.Order;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

public class QueryTest extends DataTest {
    @Test
    public void testFindOneEquals() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(MockUser.class);
        UUID id = UUID.randomUUID();
        MockUser original = MockUserFactory.builder(dataManager)
                .id(id)
                .name("test user")
                .insert(InsertMode.SYNC);

        MockUser got = MockUserQuery.where(dataManager)
                .idIs(id)
                .findOne();
        assertSame(original, got);
    }

    @Test
    public void testFindAllLike() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(MockUser.class);

        MockUser original1 = MockUserFactory.builder(dataManager)
                .id(UUID.randomUUID())
                .name("test user")
                .age(0)
                .insert(InsertMode.SYNC);
        MockUser original2 = MockUserFactory.builder(dataManager)
                .id(UUID.randomUUID())
                .name("test user2")
                .age(5)
                .insert(InsertMode.SYNC);

        List<MockUser> got = MockUserQuery.where(dataManager)
                .nameIsLike("%test user%")
                .orderByAge(Order.ASCENDING)
                .findAll();

        assertEquals(2, got.size());
        assertTrue(got.contains(original1));
        assertTrue(got.contains(original2));

        assertSame(original1, got.get(0));
        assertSame(original2, got.get(1));

        got = MockUserQuery.where(dataManager)
                .nameIsLike("%test user%")
                .orderByAge(Order.DESCENDING)
                .findAll();

        assertEquals(2, got.size());
        assertTrue(got.contains(original1));
        assertTrue(got.contains(original2));

        assertSame(original2, got.get(0));
        assertSame(original1, got.get(1));
    }

    @Test
    public void testQueryOnForeignColumn() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(MockUser.class);

        MockUser likesRed = MockUserFactory.builder(dataManager)
                .id(UUID.randomUUID())
                .name("Likes Red")
                .favoriteColor("red")
                .insert(InsertMode.SYNC);
        MockUser likesGreen = MockUserFactory.builder(dataManager)
                .id(UUID.randomUUID())
                .name("Likes Green")
                .favoriteColor("green")
                .insert(InsertMode.SYNC);

        assertNull(MockUserQuery.where(dataManager)
                .favoriteColorIs("blue")
                .findOne());

        assertSame(likesRed, MockUserQuery.where(dataManager)
                .favoriteColorIs("red")
                .findOne());

        assertSame(likesGreen, MockUserQuery.where(dataManager)
                .favoriteColorIs("green")
                .findOne());

        List<MockUser> users = MockUserQuery.where(dataManager)
                .favoriteColorIsIn("red", "green")
                .orderByName(Order.ASCENDING)
                .findAll();
        assertEquals(2, users.size());
        assertSame(likesGreen, users.get(0));
        assertSame(likesRed, users.get(1));

        users = MockUserQuery.where(dataManager)
                .favoriteColorIsNotNull()
                .orderByFavoriteColor(Order.DESCENDING)
                .findAll();
        assertEquals(2, users.size());
        assertSame(likesRed, users.get(0));
        assertSame(likesGreen, users.get(1));
    }

    @Test
    public void testEqualsClause() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        assertEquals("WHERE \"public\".\"users\".\"id\" = ?", MockUserQuery.where(dataManager).idIs(UUID.randomUUID()).toString());
    }

    @Test
    public void testBetweenClause() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        assertEquals("WHERE \"public\".\"users\".\"age\" BETWEEN ? AND ?", MockUserQuery.where(dataManager).ageIsBetween(0, 0).toString());
    }

    @Test
    public void testAgeIsLessThanClause() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        assertEquals("WHERE \"public\".\"users\".\"age\" < ?", MockUserQuery.where(dataManager).ageIsLessThan(0).toString());
    }

    @Test
    public void testAgeIsLessThanOrEqualToClause() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        assertEquals("WHERE \"public\".\"users\".\"age\" <= ?", MockUserQuery.where(dataManager).ageIsLessThanOrEqualTo(0).toString());
    }

    @Test
    public void testAgeIsGreaterThanClause() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        assertEquals("WHERE \"public\".\"users\".\"age\" > ?", MockUserQuery.where(dataManager).ageIsGreaterThan(0).toString());
    }

    @Test
    public void testAgeIsGreaterThanOrEqualToClause() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        assertEquals("WHERE \"public\".\"users\".\"age\" >= ?", MockUserQuery.where(dataManager).ageIsGreaterThanOrEqualTo(0).toString());
    }

    @Test
    public void testAgeIsNullClause() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        assertEquals("WHERE \"public\".\"users\".\"age\" IS NULL", MockUserQuery.where(dataManager).ageIsNull().toString());
    }

    @Test
    public void testAgeIsNotNullClause() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        assertEquals("WHERE \"public\".\"users\".\"age\" IS NOT NULL", MockUserQuery.where(dataManager).ageIsNotNull().toString());
    }

    @Test
    public void testNameIsLikeClause() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        assertEquals("WHERE \"public\".\"users\".\"name\" LIKE ?", MockUserQuery.where(dataManager).nameIsLike("%test%").toString());
    }

    @Test
    public void testNameIsNotLikeClause() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        assertEquals("WHERE \"public\".\"users\".\"name\" NOT LIKE ?", MockUserQuery.where(dataManager).nameIsNotLike("%test%").toString());
    }

    @Test
    public void testNameIsInClause() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        assertEquals("WHERE \"public\".\"users\".\"name\" IN (?, ?, ?)", MockUserQuery.where(dataManager).nameIsIn("name1", "name2", "name3").toString());
    }

    @Test
    public void testNameIsInListClause() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        assertEquals("WHERE \"public\".\"users\".\"name\" IN (?, ?, ?)", MockUserQuery.where(dataManager).nameIsIn(List.of("name1", "name2", "name3")).toString());
    }

    @Test
    public void testLimitClause() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        assertEquals("WHERE \"public\".\"users\".\"id\" = ? LIMIT 10", MockUserQuery.where(dataManager).idIs(UUID.randomUUID()).limit(10).toString());
    }

    @Test
    public void testOffsetClause() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        assertEquals("WHERE \"public\".\"users\".\"id\" = ? OFFSET 5", MockUserQuery.where(dataManager).idIs(UUID.randomUUID()).offset(5).toString());
    }

    @Test
    public void testOrderByClause() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        assertEquals("WHERE \"public\".\"users\".\"id\" = ? ORDER BY \"public\".\"users\".\"age\" ASC", MockUserQuery.where(dataManager).idIs(UUID.randomUUID()).orderByAge(Order.ASCENDING).toString());
    }

    @Test
    public void testAndClauseWithoutParentheses() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        assertEquals("WHERE \"public\".\"users\".\"id\" = ? AND \"public\".\"users\".\"age\" BETWEEN ? AND ?", MockUserQuery.where(dataManager).idIs(UUID.randomUUID()).and().ageIsBetween(0, 5).toString());
    }

    @Test
    public void testOrClauseWithoutParentheses() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        assertEquals("WHERE \"public\".\"users\".\"id\" = ? OR \"public\".\"users\".\"age\" BETWEEN ? AND ?", MockUserQuery.where(dataManager).idIs(UUID.randomUUID()).or().ageIsBetween(0, 5).toString());
    }

    @Test
    public void testAndClauseWithParentheses() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        assertEquals("WHERE \"public\".\"users\".\"id\" = ? AND (\"public\".\"users\".\"age\" BETWEEN ? AND ?)", MockUserQuery.where(dataManager).idIs(UUID.randomUUID()).and(q -> q.ageIsBetween(0, 5)).toString());
    }

    @Test
    public void testOrClauseWithParentheses() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        assertEquals("WHERE \"public\".\"users\".\"id\" = ? OR (\"public\".\"users\".\"age\" BETWEEN ? AND ?)", MockUserQuery.where(dataManager).idIs(UUID.randomUUID()).or(q -> q.ageIsBetween(0, 5)).toString());
    }

    @Test
    public void testComplexClause() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        assertEquals("WHERE \"public\".\"users\".\"id\" = ? OR (\"public\".\"users\".\"age\" BETWEEN ? AND ?) AND \"public\".\"users\".\"name\" LIKE ? LIMIT 10 OFFSET 5 ORDER BY \"public\".\"users\".\"age\" DESC",
                MockUserQuery.where(dataManager)
                        .idIs(UUID.randomUUID())
                        .or(q -> q.ageIsBetween(0, 5))
                        .and()
                        .nameIsLike("%test%")
                        .orderByAge(Order.DESCENDING)
                        .limit(10)
                        .offset(5)
                        .toString());
    }
}