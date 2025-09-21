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
    public void testQuery() { //todo: test each clause and ensure it outputs the proper sql
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();

        String where = MockUserQuery.where(dataManager)
                .idIs(UUID.randomUUID())
                .or()
//                .nameIs("user name")
//                .or(q -> q.ageIsLessThan(10)
//                        .and()
//                        .ageIsLessThanOrEqualTo(5)
//                )
//                .or()
//                .nameIsIn("name1", "name2", "name3")
//                .or()
//                .nameIsIn(List.of("name4", "name5", "name6"))
//                .limit(10)
//                .offset(2)
//                .and()
//                .ageIsBetween(0, 3)
//                .and()
//                .ageIsNotNull()
//                .and()
//                .nameIsLike("%something%")
//                .and()
//                .nameIsNotLike("%nothing%")
//                .orderByAge(Order.ASCENDING)
//                .and()
                .nameUpdatesIs(4)
                .toString();
        System.out.println(where);
    }
}