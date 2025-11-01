package net.staticstudios.data;

import net.staticstudios.data.misc.DataTest;
import net.staticstudios.data.mock.user.MockUser;
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
        MockUser original = MockUser.builder(dataManager)
                .id(id)
                .name("test user")
                .insert(InsertMode.SYNC);

        MockUser got = MockUser.query(dataManager).where(w -> w.idIs(id))
                .findOne();
        assertSame(original, got);
    }

    @Test
    public void testFindAllLike() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(MockUser.class);

        MockUser original1 = MockUser.builder(dataManager)
                .id(UUID.randomUUID())
                .name("test user")
                .age(0)
                .insert(InsertMode.SYNC);
        MockUser original2 = MockUser.builder(dataManager)
                .id(UUID.randomUUID())
                .name("test user2")
                .age(5)
                .insert(InsertMode.SYNC);

        List<MockUser> got = MockUser.query(dataManager).where(w -> w.nameIsLike("%test user%"))
                .orderByAge(Order.ASCENDING)
                .findAll();

        assertEquals(2, got.size());
        assertTrue(got.contains(original1));
        assertTrue(got.contains(original2));

        assertSame(original1, got.get(0));
        assertSame(original2, got.get(1));

        got = MockUser.query(dataManager).where(w -> w.nameIsLike("%test user%"))
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

        MockUser likesRed = MockUser.builder(dataManager)
                .id(UUID.randomUUID())
                .name("Likes Red")
                .favoriteColor("red")
                .insert(InsertMode.SYNC);
        MockUser likesGreen = MockUser.builder(dataManager)
                .id(UUID.randomUUID())
                .name("Likes Green")
                .favoriteColor("green")
                .insert(InsertMode.SYNC);

        assertNull(MockUser.query(dataManager).where(w -> w.favoriteColorIs("blue"))
                .findOne());

        assertSame(likesRed, MockUser.query(dataManager).where(w -> w.favoriteColorIs("red"))
                .findOne());

        assertSame(likesGreen, MockUser.query(dataManager).where(w -> w.favoriteColorIs("green"))
                .findOne());

        List<MockUser> users = MockUser.query(dataManager).where(w -> w
                        .favoriteColorIsIn("red", "green")
                )
                .orderByName(Order.ASCENDING)
                .findAll();
        assertEquals(2, users.size());
        assertSame(likesGreen, users.get(0));
        assertSame(likesRed, users.get(1));

        users = MockUser.query(dataManager).where(w -> w.favoriteColorIsNotNull())
                .orderByFavoriteColor(Order.DESCENDING)
                .findAll();
        assertEquals(2, users.size());
        assertSame(likesRed, users.get(0));
        assertSame(likesGreen, users.get(1));
    }

    @Test
    public void testEqualsClause() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        assertEquals("WHERE \"public\".\"users\".\"id\" = ?", MockUser.query(dataManager).where(w -> w.idIs(UUID.randomUUID())).toString());
    }

    @Test
    public void testBetweenClause() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        assertEquals("WHERE \"public\".\"users\".\"age\" BETWEEN ? AND ?", MockUser.query(dataManager).where(w -> w.ageIsBetween(0, 0)).toString());
    }

    @Test
    public void testAgeIsLessThanClause() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        assertEquals("WHERE \"public\".\"users\".\"age\" < ?", MockUser.query(dataManager).where(w -> w.ageIsLessThan(0)).toString());
    }

    @Test
    public void testAgeIsLessThanOrEqualToClause() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        assertEquals("WHERE \"public\".\"users\".\"age\" <= ?", MockUser.query(dataManager).where(w -> w.ageIsLessThanOrEqualTo(0)).toString());
    }

    @Test
    public void testAgeIsGreaterThanClause() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        assertEquals("WHERE \"public\".\"users\".\"age\" > ?", MockUser.query(dataManager).where(w -> w.ageIsGreaterThan(0)).toString());
    }

    @Test
    public void testAgeIsGreaterThanOrEqualToClause() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        assertEquals("WHERE \"public\".\"users\".\"age\" >= ?", MockUser.query(dataManager).where(w -> w.ageIsGreaterThanOrEqualTo(0)).toString());
    }

    @Test
    public void testAgeIsNullClause() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        assertEquals("WHERE \"public\".\"users\".\"age\" IS NULL", MockUser.query(dataManager).where(w -> w.ageIsNull()).toString());
    }

    @Test
    public void testAgeIsNotNullClause() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        assertEquals("WHERE \"public\".\"users\".\"age\" IS NOT NULL", MockUser.query(dataManager).where(w -> w.ageIsNotNull()).toString());
    }

    @Test
    public void testNameIsLikeClause() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        assertEquals("WHERE \"public\".\"users\".\"name\" LIKE ?", MockUser.query(dataManager).where(w -> w.nameIsLike("%test%")).toString());
    }

    @Test
    public void testNameIsNotLikeClause() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        assertEquals("WHERE \"public\".\"users\".\"name\" NOT LIKE ?", MockUser.query(dataManager).where(w -> w.nameIsNotLike("%test%")).toString());
    }

    @Test
    public void testNameIsInClause() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        assertEquals("WHERE \"public\".\"users\".\"name\" IN (?, ?, ?)", MockUser.query(dataManager).where(w -> w.nameIsIn("name1", "name2", "name3")).toString());
    }

    @Test
    public void testNameIsInListClause() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        assertEquals("WHERE \"public\".\"users\".\"name\" IN (?, ?, ?)", MockUser.query(dataManager).where(w -> w.nameIsIn(List.of("name1", "name2", "name3"))).toString());
    }

    @Test
    public void testLimitClause() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        assertEquals("WHERE \"public\".\"users\".\"id\" = ? LIMIT 10", MockUser.query(dataManager).where(w -> w.idIs(UUID.randomUUID())).limit(10).toString());
    }

    @Test
    public void testOffsetClause() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        assertEquals("WHERE \"public\".\"users\".\"id\" = ? OFFSET 5", MockUser.query(dataManager).where(w -> w.idIs(UUID.randomUUID())).offset(5).toString());
    }

    @Test
    public void testOrderByClause() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        assertEquals("WHERE \"public\".\"users\".\"id\" = ? ORDER BY \"public\".\"users\".\"age\" ASC", MockUser.query(dataManager).where(w -> w.idIs(UUID.randomUUID())).orderByAge(Order.ASCENDING).toString());
    }

    @Test
    public void testAndClause() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        assertEquals("WHERE (\"public\".\"users\".\"id\" = ? AND \"public\".\"users\".\"age\" BETWEEN ? AND ?)", MockUser.query(dataManager).where(w -> w.idIs(UUID.randomUUID())
                .and()
                .group(w1 -> w1.ageIsBetween(0, 5))
        ).toString());
    }

    @Test
    public void testOrClause() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        assertEquals("WHERE (\"public\".\"users\".\"id\" = ? OR \"public\".\"users\".\"age\" BETWEEN ? AND ?)", MockUser.query(dataManager).where(w -> w.idIs(UUID.randomUUID())
                .or()
                .ageIsBetween(0, 5)).toString());
    }

    @Test
    public void testComplexClause() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        assertEquals("WHERE ((\"public\".\"users\".\"id\" = ? OR \"public\".\"users\".\"age\" BETWEEN ? AND ?) AND \"public\".\"users\".\"name\" LIKE ?) LIMIT 10 OFFSET 5 ORDER BY \"public\".\"users\".\"age\" DESC",
                MockUser.query(dataManager).where(w -> w
                                .idIs(UUID.randomUUID())
                                .or()
                                .ageIsBetween(0, 5)
                                .and()
                                .nameIsLike("%test%")
                        )
                        .orderByAge(Order.DESCENDING)
                        .limit(10)
                        .offset(5)
                        .toString());
    }

    //todo: test more complex cases
}