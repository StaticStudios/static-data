package net.staticstudios.data;

import net.staticstudios.data.misc.DataTest;
import net.staticstudios.data.misc.MockEnvironment;
import net.staticstudios.data.mock.persistentcollection.FacebookPost;
import net.staticstudios.data.mock.persistentcollection.FacebookUser;
import org.junit.jupiter.api.BeforeEach;
import org.junitpioneer.jupiter.RetryingTest;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class PersistentCollectionTest extends DataTest {

    //todo: we need to add add handlers add and remove handlers
    //todo: we need to test what happens when we manually edit the db. do the collections update?

    @BeforeEach
    public void init() {
        try (Statement statement = getConnection().createStatement()) {
            statement.executeUpdate("""
                    drop schema if exists facebook cascade;
                    create schema if not exists facebook;
                    create table if not exists facebook.users (
                                                    id uuid primary key
                    );
                    create table if not exists facebook.posts (
                                                    id uuid primary key,
                                                    user_id uuid,
                                                    description text not null default '',
                                                    likes int not null default 0
                    );
                    create table if not exists facebook.favorite_quotes (
                                                              id uuid primary key,
                                                              user_id uuid,
                                                              quote text not null default ''
                    );
                    """);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        getMockEnvironments().forEach(env -> {
            DataManager dataManager = env.dataManager();
            dataManager.loadAll(FacebookUser.class);
        });
    }

    @RetryingTest(5)
    public void testAddToUniqueDataCollection() {
        MockEnvironment mockEnvironment = getMockEnvironments().getFirst();
        DataManager dataManager = mockEnvironment.dataManager();

        FacebookUser facebookUser = FacebookUser.createSync(dataManager);

        FacebookPost post1 = FacebookPost.createSync(dataManager, "Here's some post description", facebookUser);
        FacebookPost post2 = FacebookPost.createSync(dataManager, "Here's some post description", facebookUser);

        assertEquals(2, facebookUser.getPosts().size());
        assertTrue(facebookUser.getPosts().contains(post1));
        assertTrue(facebookUser.getPosts().contains(post2));
        assertEquals(facebookUser, post1.getUser());
        assertEquals(facebookUser, post2.getUser());

        FacebookPost post3 = FacebookPost.createSync(dataManager, "Here's some post description", null);
        facebookUser.getPosts().add(post3);

        assertEquals(3, facebookUser.getPosts().size());
        assertTrue(facebookUser.getPosts().contains(post3));
        assertEquals(facebookUser, post3.getUser());

        waitForDataPropagation();

        try (PreparedStatement statement = getConnection().prepareStatement("SELECT * FROM facebook.posts WHERE id = ?")) {
            statement.setObject(1, post1.getId());
            assertTrue(statement.execute());
            ResultSet resultSet = statement.getResultSet();
            assertTrue(resultSet.next());
            assertEquals(facebookUser.getId(), resultSet.getObject("user_id"));
            assertEquals("Here's some post description", resultSet.getString("description"));
            assertEquals(0, resultSet.getInt("likes"));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        try (PreparedStatement statement = getConnection().prepareStatement("SELECT * FROM facebook.posts WHERE id = ?")) {
            statement.setObject(1, post3.getId());
            assertTrue(statement.execute());
            ResultSet resultSet = statement.getResultSet();
            assertTrue(resultSet.next());
            assertEquals(facebookUser.getId(), resultSet.getObject("user_id"));
            assertEquals("Here's some post description", resultSet.getString("description"));
            assertEquals(0, resultSet.getInt("likes"));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @RetryingTest(5)
    public void testAddAllToUniqueDataCollection() {
        MockEnvironment mockEnvironment = getMockEnvironments().getFirst();
        DataManager dataManager = mockEnvironment.dataManager();

        FacebookUser facebookUser = FacebookUser.createSync(dataManager);

        FacebookPost post1 = FacebookPost.createSync(dataManager, "Here's some post description", null);
        FacebookPost post2 = FacebookPost.createSync(dataManager, "Here's some post description", null);

        assertEquals(0, facebookUser.getPosts().size());

        facebookUser.getPosts().addAll(List.of(post1, post2));

        assertEquals(2, facebookUser.getPosts().size());

        assertTrue(facebookUser.getPosts().contains(post1));
        assertTrue(facebookUser.getPosts().contains(post2));
        assertEquals(facebookUser, post1.getUser());
        assertEquals(facebookUser, post2.getUser());

        waitForDataPropagation();

        try (PreparedStatement statement = getConnection().prepareStatement("SELECT * FROM facebook.posts WHERE id = ?")) {
            statement.setObject(1, post1.getId());
            assertTrue(statement.execute());
            ResultSet resultSet = statement.getResultSet();
            assertTrue(resultSet.next());
            assertEquals(facebookUser.getId(), resultSet.getObject("user_id"));
            assertEquals("Here's some post description", resultSet.getString("description"));
            assertEquals(0, resultSet.getInt("likes"));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @RetryingTest(5)
    public void testRemoveFromUniqueDataCollection() {
        MockEnvironment mockEnvironment = getMockEnvironments().getFirst();
        DataManager dataManager = mockEnvironment.dataManager();

        FacebookUser facebookUser = FacebookUser.createSync(dataManager);

        FacebookPost post1 = FacebookPost.createSync(dataManager, "Here's some post description", facebookUser);
        FacebookPost post2 = FacebookPost.createSync(dataManager, "Here's some post description", facebookUser);

        assertEquals(2, facebookUser.getPosts().size());
        assertTrue(facebookUser.getPosts().contains(post1));
        assertTrue(facebookUser.getPosts().contains(post2));
        assertEquals(facebookUser, post1.getUser());
        assertEquals(facebookUser, post2.getUser());

        waitForDataPropagation();

        assertTrue(facebookUser.getPosts().remove(post1));

        assertEquals(1, facebookUser.getPosts().size());
        assertTrue(facebookUser.getPosts().contains(post2));
        assertEquals(facebookUser, post2.getUser());

        waitForDataPropagation();

        try (PreparedStatement statement = getConnection().prepareStatement("SELECT * FROM facebook.posts WHERE id = ?")) {
            statement.setObject(1, post1.getId());
            assertTrue(statement.execute());
            ResultSet resultSet = statement.getResultSet();
            assertTrue(resultSet.next());
            assertNull(resultSet.getObject("user_id"));
            assertEquals("Here's some post description", resultSet.getString("description"));
            assertEquals(0, resultSet.getInt("likes"));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }


        assertEquals(1, facebookUser.getPosts().size());
        dataManager.delete(post2);

        assertEquals(0, facebookUser.getPosts().size());

        waitForDataPropagation();

        try (PreparedStatement statement = getConnection().prepareStatement("SELECT * FROM facebook.posts WHERE id = ?")) {
            statement.setObject(1, post2.getId());
            assertTrue(statement.execute());
            ResultSet resultSet = statement.getResultSet();
            assertFalse(resultSet.next());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @RetryingTest(5)
    public void testRemoveAllFromUniqueDataCollection() {
        MockEnvironment mockEnvironment = getMockEnvironments().getFirst();
        DataManager dataManager = mockEnvironment.dataManager();

        FacebookUser facebookUser = FacebookUser.createSync(dataManager);

        FacebookPost post1 = FacebookPost.createSync(dataManager, "Here's some post description", facebookUser);
        FacebookPost post2 = FacebookPost.createSync(dataManager, "Here's some post description", facebookUser);
        FacebookPost post3 = FacebookPost.createSync(dataManager, "Here's some post description", facebookUser);

        assertEquals(3, facebookUser.getPosts().size());
        assertTrue(facebookUser.getPosts().contains(post1));
        assertTrue(facebookUser.getPosts().contains(post2));
        assertTrue(facebookUser.getPosts().contains(post3));
        assertEquals(facebookUser, post1.getUser());
        assertEquals(facebookUser, post2.getUser());
        assertEquals(facebookUser, post3.getUser());

        waitForDataPropagation();

        assertTrue(facebookUser.getPosts().removeAll(List.of(post1, post2)));

        assertEquals(1, facebookUser.getPosts().size());
        assertTrue(facebookUser.getPosts().contains(post3));

        waitForDataPropagation();

        try (PreparedStatement statement = getConnection().prepareStatement("SELECT * FROM facebook.posts WHERE user_id = ?")) {
            statement.setObject(1, facebookUser.getId());
            assertTrue(statement.execute());
            ResultSet resultSet = statement.getResultSet();
            assertTrue(resultSet.next());
            assertFalse(resultSet.next());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @RetryingTest(5)
    public void testContainsInUniqueDataCollection() {
        MockEnvironment mockEnvironment = getMockEnvironments().getFirst();
        DataManager dataManager = mockEnvironment.dataManager();

        FacebookUser facebookUser = FacebookUser.createSync(dataManager);

        FacebookPost post1 = FacebookPost.createSync(dataManager, "Here's some post description", facebookUser);

        assertEquals(1, facebookUser.getPosts().size());
        FacebookPost post2 = FacebookPost.createSync(dataManager, "Here's some post description", null);
        facebookUser.getPosts().add(post2);
        assertEquals(2, facebookUser.getPosts().size());

        FacebookPost post3 = FacebookPost.createSync(dataManager, "Here's some post description", null);

        assertTrue(facebookUser.getPosts().contains(post1));
        assertTrue(facebookUser.getPosts().contains(post2));

        assertFalse(facebookUser.getPosts().contains(post3));
        assertFalse(facebookUser.getPosts().contains("Not a post"));
        assertFalse(facebookUser.getPosts().contains(null));
    }

    @RetryingTest(5)
    public void testContainsAllInUniqueDataCollection() {
        MockEnvironment mockEnvironment = getMockEnvironments().getFirst();
        DataManager dataManager = mockEnvironment.dataManager();

        FacebookUser facebookUser = FacebookUser.createSync(dataManager);

        FacebookPost post1 = FacebookPost.createSync(dataManager, "Here's some post description", facebookUser);
        FacebookPost post2 = FacebookPost.createSync(dataManager, "Here's some post description", facebookUser);

        assertEquals(2, facebookUser.getPosts().size());

        FacebookPost post3 = FacebookPost.createSync(dataManager, "Here's some post description", null);

        assertTrue(facebookUser.getPosts().contains(post1));
        assertTrue(facebookUser.getPosts().contains(post2));
        assertTrue(facebookUser.getPosts().containsAll(List.of(post1, post2)));
        assertFalse(facebookUser.getPosts().containsAll(List.of(post1, post2, post3)));

        assertFalse(facebookUser.getPosts().containsAll(List.of(post1, post2, "Not a post")));
        List<Object> bad = new ArrayList<>();
        bad.add(post1);
        bad.add(post2);
        bad.add(null);
        assertFalse(facebookUser.getPosts().containsAll(bad));
        assertTrue(facebookUser.getPosts().containsAll(List.of()));
    }

    @RetryingTest(5)
    public void testRetainAllInUniqueDataCollection() {
        MockEnvironment mockEnvironment = getMockEnvironments().getFirst();
        DataManager dataManager = mockEnvironment.dataManager();

        FacebookUser facebookUser = FacebookUser.createSync(dataManager);

        FacebookPost post1 = FacebookPost.createSync(dataManager, "Here's some post description", facebookUser);
        FacebookPost post2 = FacebookPost.createSync(dataManager, "Here's some post description", facebookUser);
        FacebookPost post3 = FacebookPost.createSync(dataManager, "Here's some post description", null);

        assertEquals(2, facebookUser.getPosts().size());
        assertTrue(facebookUser.getPosts().contains(post1));
        assertTrue(facebookUser.getPosts().contains(post2));
        assertFalse(facebookUser.getPosts().contains(post3));

        assertTrue(facebookUser.getPosts().retainAll(List.of(post1, post3)));
        assertEquals(1, facebookUser.getPosts().size());
        assertTrue(facebookUser.getPosts().contains(post1));
        assertFalse(facebookUser.getPosts().contains(post2));
        assertFalse(facebookUser.getPosts().contains(post3));

        assertFalse(facebookUser.getPosts().retainAll(List.of(post1)));
        assertEquals(1, facebookUser.getPosts().size());
        assertTrue(facebookUser.getPosts().contains(post1));
        assertFalse(facebookUser.getPosts().contains(post2));
        assertFalse(facebookUser.getPosts().contains(post3));
    }

    @RetryingTest(5)
    public void testToArrayInUniqueDataCollection() {
        MockEnvironment mockEnvironment = getMockEnvironments().getFirst();
        DataManager dataManager = mockEnvironment.dataManager();

        FacebookUser facebookUser = FacebookUser.createSync(dataManager);

        FacebookPost post1 = FacebookPost.createSync(dataManager, "Here's some post description", facebookUser);
        FacebookPost post2 = FacebookPost.createSync(dataManager, "Here's some post description", facebookUser);

        assertEquals(2, facebookUser.getPosts().size());

        FacebookPost[] posts = facebookUser.getPosts().toArray(new FacebookPost[0]);
        assertEquals(2, posts.length);
        boolean containsPost1 = false;
        boolean containsPost2 = false;

        for (FacebookPost post : posts) {
            if (post.equals(post1)) {
                containsPost1 = true;
            } else if (post.equals(post2)) {
                containsPost2 = true;
            }
        }

        assertTrue(containsPost1);
        assertTrue(containsPost2);

        Object[] objects = facebookUser.getPosts().toArray();
        assertEquals(2, objects.length);
        boolean containsPost1Object = false;
        boolean containsPost2Object = false;

        for (Object object : objects) {
            if (object.equals(post1)) {
                containsPost1Object = true;
            } else if (object.equals(post2)) {
                containsPost2Object = true;
            }
        }

        assertTrue(containsPost1Object);
        assertTrue(containsPost2Object);
    }

    @RetryingTest(5)
    public void testIteratorInUniqueDataCollection() {
        MockEnvironment mockEnvironment = getMockEnvironments().getFirst();
        DataManager dataManager = mockEnvironment.dataManager();

        FacebookUser facebookUser = FacebookUser.createSync(dataManager);

        FacebookPost post1 = FacebookPost.createSync(dataManager, "Here's some post description", facebookUser);
        FacebookPost post2 = FacebookPost.createSync(dataManager, "Here's some post description", facebookUser);

        assertEquals(2, facebookUser.getPosts().size());

        Iterator<FacebookPost> iterator = facebookUser.getPosts().iterator();
        assertTrue(iterator.hasNext());
        FacebookPost next = iterator.next();
        assertTrue(next.equals(post1) || next.equals(post2));
        assertTrue(iterator.hasNext());
        next = iterator.next();
        assertTrue(next.equals(post1) || next.equals(post2));
        assertFalse(iterator.hasNext());

        iterator = facebookUser.getPosts().iterator();
        assertTrue(iterator.hasNext());
        iterator.next();
        iterator.remove();

        assertEquals(1, facebookUser.getPosts().size());

        assertTrue(iterator.hasNext());
        iterator.next();
        iterator.remove();

        assertEquals(0, facebookUser.getPosts().size());

        assertFalse(iterator.hasNext());

        waitForDataPropagation();

        try (PreparedStatement statement = getConnection().prepareStatement("SELECT * FROM facebook.posts WHERE user_id = ?")) {
            statement.setObject(1, facebookUser.getId());
            assertTrue(statement.execute());
            ResultSet resultSet = statement.getResultSet();
            assertFalse(resultSet.next());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    // ----- Test PersistentValueCollections ----- //

    @RetryingTest(5)
    public void testAddToValueCollection() {
        MockEnvironment mockEnvironment = getMockEnvironments().getFirst();
        DataManager dataManager = mockEnvironment.dataManager();

        FacebookUser facebookUser = FacebookUser.createSync(dataManager);

        //Duplicates are permitted
        facebookUser.getFavoriteQuotes().add("Here's a quote");
        facebookUser.getFavoriteQuotes().add("Here's a quote");
        facebookUser.getFavoriteQuotes().add("Here's another quote");

        assertEquals(3, facebookUser.getFavoriteQuotes().size());
        assertTrue(facebookUser.getFavoriteQuotes().contains("Here's a quote"));
        assertTrue(facebookUser.getFavoriteQuotes().contains("Here's another quote"));

        waitForDataPropagation();

        try (PreparedStatement statement = getConnection().prepareStatement("SELECT * FROM facebook.favorite_quotes WHERE user_id = ?")) {
            statement.setObject(1, facebookUser.getId());
            assertTrue(statement.execute());
            List<String> quotes = new ArrayList<>();
            ResultSet resultSet = statement.getResultSet();
            while (resultSet.next()) {
                quotes.add(resultSet.getString("quote"));
            }

            assertEquals(3, quotes.size());
            assertTrue(quotes.contains("Here's a quote"));
            assertTrue(quotes.contains("Here's another quote"));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @RetryingTest(5)
    public void testAddAllToValueCollection() {
        MockEnvironment mockEnvironment = getMockEnvironments().getFirst();
        DataManager dataManager = mockEnvironment.dataManager();

        FacebookUser facebookUser = FacebookUser.createSync(dataManager);

        //Duplicates are permitted
        facebookUser.getFavoriteQuotes().addAll(List.of("Here's a quote", "Here's a quote", "Here's another quote"));

        assertEquals(3, facebookUser.getFavoriteQuotes().size());
        assertTrue(facebookUser.getFavoriteQuotes().contains("Here's a quote"));
        assertTrue(facebookUser.getFavoriteQuotes().contains("Here's another quote"));

        waitForDataPropagation();

        try (PreparedStatement statement = getConnection().prepareStatement("SELECT * FROM facebook.favorite_quotes WHERE user_id = ?")) {
            statement.setObject(1, facebookUser.getId());
            assertTrue(statement.execute());
            List<String> quotes = new ArrayList<>();
            ResultSet resultSet = statement.getResultSet();
            while (resultSet.next()) {
                quotes.add(resultSet.getString("quote"));
            }

            assertEquals(3, quotes.size());
            assertTrue(quotes.contains("Here's a quote"));
            assertTrue(quotes.contains("Here's another quote"));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @RetryingTest(5)
    public void testRemoveFromValueCollection() {
        MockEnvironment mockEnvironment = getMockEnvironments().getFirst();
        DataManager dataManager = mockEnvironment.dataManager();

        FacebookUser facebookUser = FacebookUser.createSync(dataManager);

        //Duplicates are permitted
        facebookUser.getFavoriteQuotes().add("Here's a quote");
        facebookUser.getFavoriteQuotes().add("Here's a quote");
        facebookUser.getFavoriteQuotes().add("Here's another quote");

        assertEquals(3, facebookUser.getFavoriteQuotes().size());
        assertTrue(facebookUser.getFavoriteQuotes().contains("Here's a quote"));
        assertTrue(facebookUser.getFavoriteQuotes().contains("Here's another quote"));

        assertTrue(facebookUser.getFavoriteQuotes().remove("Here's a quote"));

        assertEquals(2, facebookUser.getFavoriteQuotes().size());
        assertTrue(facebookUser.getFavoriteQuotes().contains("Here's a quote"));
        assertTrue(facebookUser.getFavoriteQuotes().contains("Here's another quote"));

        assertTrue(facebookUser.getFavoriteQuotes().remove("Here's a quote"));

        assertEquals(1, facebookUser.getFavoriteQuotes().size());
        assertTrue(facebookUser.getFavoriteQuotes().contains("Here's another quote"));

        assertFalse(facebookUser.getFavoriteQuotes().remove("Here's a quote"));

        waitForDataPropagation();

        try (PreparedStatement statement = getConnection().prepareStatement("SELECT * FROM facebook.favorite_quotes WHERE user_id = ?")) {
            statement.setObject(1, facebookUser.getId());
            assertTrue(statement.execute());
            List<String> quotes = new ArrayList<>();
            ResultSet resultSet = statement.getResultSet();
            while (resultSet.next()) {
                quotes.add(resultSet.getString("quote"));
            }

            assertEquals(1, quotes.size());
            assertTrue(quotes.contains("Here's another quote"));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @RetryingTest(5)
    public void testRemoveAllFromValueCollection() {
        MockEnvironment mockEnvironment = getMockEnvironments().getFirst();
        DataManager dataManager = mockEnvironment.dataManager();

        FacebookUser facebookUser = FacebookUser.createSync(dataManager);

        //Duplicates are permitted
        facebookUser.getFavoriteQuotes().add("Here's a quote");
        facebookUser.getFavoriteQuotes().add("Here's a quote");
        facebookUser.getFavoriteQuotes().add("Here's another quote");

        assertEquals(3, facebookUser.getFavoriteQuotes().size());
        assertTrue(facebookUser.getFavoriteQuotes().contains("Here's a quote"));
        assertTrue(facebookUser.getFavoriteQuotes().contains("Here's another quote"));

        assertTrue(facebookUser.getFavoriteQuotes().removeAll(List.of("Here's a quote", "Here's another quote")));

        assertEquals(1, facebookUser.getFavoriteQuotes().size());
        assertTrue(facebookUser.getFavoriteQuotes().contains("Here's a quote"));

        waitForDataPropagation();

        try (PreparedStatement statement = getConnection().prepareStatement("SELECT * FROM facebook.favorite_quotes WHERE user_id = ?")) {
            statement.setObject(1, facebookUser.getId());
            assertTrue(statement.execute());
            List<String> quotes = new ArrayList<>();
            ResultSet resultSet = statement.getResultSet();
            while (resultSet.next()) {
                quotes.add(resultSet.getString("quote"));
            }

            assertEquals(1, quotes.size());
            assertTrue(quotes.contains("Here's a quote"));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @RetryingTest(5)
    public void testContainsInValueCollection() {
        MockEnvironment mockEnvironment = getMockEnvironments().getFirst();
        DataManager dataManager = mockEnvironment.dataManager();

        FacebookUser facebookUser = FacebookUser.createSync(dataManager);

        //Duplicates are permitted
        facebookUser.getFavoriteQuotes().add("Here's a quote");
        facebookUser.getFavoriteQuotes().add("Here's a quote");
        facebookUser.getFavoriteQuotes().add("Here's another quote");

        assertEquals(3, facebookUser.getFavoriteQuotes().size());
        assertTrue(facebookUser.getFavoriteQuotes().contains("Here's a quote"));
        assertTrue(facebookUser.getFavoriteQuotes().contains("Here's another quote"));

        assertFalse(facebookUser.getFavoriteQuotes().contains("Not a quote"));
        assertFalse(facebookUser.getFavoriteQuotes().contains(null));
    }

    @RetryingTest(5)
    public void testContainsAllInValueCollection() {
        MockEnvironment mockEnvironment = getMockEnvironments().getFirst();
        DataManager dataManager = mockEnvironment.dataManager();

        FacebookUser facebookUser = FacebookUser.createSync(dataManager);

        //Duplicates are permitted
        facebookUser.getFavoriteQuotes().add("Here's a quote");
        facebookUser.getFavoriteQuotes().add("Here's a quote");
        facebookUser.getFavoriteQuotes().add("Here's another quote");

        assertEquals(3, facebookUser.getFavoriteQuotes().size());
        assertTrue(facebookUser.getFavoriteQuotes().contains("Here's a quote"));
        assertTrue(facebookUser.getFavoriteQuotes().contains("Here's another quote"));

        assertTrue(facebookUser.getFavoriteQuotes().containsAll(List.of("Here's a quote", "Here's another quote")));
        assertFalse(facebookUser.getFavoriteQuotes().containsAll(List.of("Here's a quote", "Here's another quote", "Not a quote")));
        List<String> bad = new ArrayList<>();
        bad.add("Here's a quote");
        bad.add("Here's another quote");
        bad.add(null);
        assertFalse(facebookUser.getFavoriteQuotes().containsAll(bad));
        assertTrue(facebookUser.getFavoriteQuotes().containsAll(List.of()));
    }

    @RetryingTest(5)
    public void testRetainAllInValueCollection() {
        MockEnvironment mockEnvironment = getMockEnvironments().getFirst();
        DataManager dataManager = mockEnvironment.dataManager();

        FacebookUser facebookUser = FacebookUser.createSync(dataManager);

        //Duplicates are permitted
        facebookUser.getFavoriteQuotes().add("Here's a quote");
        facebookUser.getFavoriteQuotes().add("Here's a quote");
        facebookUser.getFavoriteQuotes().add("Here's another quote");

        assertEquals(3, facebookUser.getFavoriteQuotes().size());
        assertTrue(facebookUser.getFavoriteQuotes().contains("Here's a quote"));
        assertTrue(facebookUser.getFavoriteQuotes().contains("Here's another quote"));

        assertFalse(facebookUser.getFavoriteQuotes().retainAll(List.of("Here's a quote", "Here's another quote")));

        assertEquals(3, facebookUser.getFavoriteQuotes().size());
        assertTrue(facebookUser.getFavoriteQuotes().contains("Here's a quote"));
        assertTrue(facebookUser.getFavoriteQuotes().contains("Here's another quote"));

        assertTrue(facebookUser.getFavoriteQuotes().retainAll(List.of("Here's a quote")));

        assertEquals(2, facebookUser.getFavoriteQuotes().size());
        assertTrue(facebookUser.getFavoriteQuotes().contains("Here's a quote"));
        assertFalse(facebookUser.getFavoriteQuotes().contains("Here's another quote"));
    }

    @RetryingTest(5)
    public void testToArrayInValueCollection() {
        MockEnvironment mockEnvironment = getMockEnvironments().getFirst();
        DataManager dataManager = mockEnvironment.dataManager();

        FacebookUser facebookUser = FacebookUser.createSync(dataManager);

        //Duplicates are permitted
        facebookUser.getFavoriteQuotes().add("Here's a quote");
        facebookUser.getFavoriteQuotes().add("Here's a quote");
        facebookUser.getFavoriteQuotes().add("Here's another quote");

        assertEquals(3, facebookUser.getFavoriteQuotes().size());

        String[] quotes = facebookUser.getFavoriteQuotes().toArray(new String[0]);
        assertEquals(3, quotes.length);
        int quote1Count = 0;
        boolean containsQuote2 = false;

        for (String quote : quotes) {
            if (quote.equals("Here's a quote")) {
                quote1Count++;
            } else if (quote.equals("Here's another quote")) {
                containsQuote2 = true;
            }
        }

        assertEquals(2, quote1Count);
        assertTrue(containsQuote2);

        Object[] objects = facebookUser.getFavoriteQuotes().toArray();
        assertEquals(3, objects.length);
        int quote1CountObject = 0;
        boolean containsQuote2Object = false;

        for (Object object : objects) {
            if (object.equals("Here's a quote")) {
                quote1CountObject++;
            } else if (object.equals("Here's another quote")) {
                containsQuote2Object = true;
            }
        }

        assertEquals(2, quote1CountObject);
        assertTrue(containsQuote2Object);
    }

    @RetryingTest(5)
    public void testIteratorInValueCollection() {
        MockEnvironment mockEnvironment = getMockEnvironments().getFirst();
        DataManager dataManager = mockEnvironment.dataManager();

        FacebookUser facebookUser = FacebookUser.createSync(dataManager);

        //Duplicates are permitted
        facebookUser.getFavoriteQuotes().add("Here's a quote");
        facebookUser.getFavoriteQuotes().add("Here's a quote");
        facebookUser.getFavoriteQuotes().add("Here's another quote");

        assertEquals(3, facebookUser.getFavoriteQuotes().size());

        Iterator<String> iterator = facebookUser.getFavoriteQuotes().iterator();
        assertTrue(iterator.hasNext());
        String next = iterator.next();
        assertTrue(next.equals("Here's a quote") || next.equals("Here's another quote"));
        assertTrue(iterator.hasNext());
        next = iterator.next();
        assertTrue(next.equals("Here's a quote") || next.equals("Here's another quote"));
        assertTrue(iterator.hasNext());
        next = iterator.next();
        assertTrue(next.equals("Here's a quote") || next.equals("Here's another quote"));
        assertFalse(iterator.hasNext());

        iterator = facebookUser.getFavoriteQuotes().iterator();
        assertTrue(iterator.hasNext());
        iterator.next();
        iterator.remove();

        assertEquals(2, facebookUser.getFavoriteQuotes().size());

        assertTrue(iterator.hasNext());
        iterator.next();
        iterator.remove();

        assertEquals(1, facebookUser.getFavoriteQuotes().size());

        assertTrue(iterator.hasNext());
        iterator.next();
        iterator.remove();

        assertEquals(0, facebookUser.getFavoriteQuotes().size());

        assertFalse(iterator.hasNext());

        waitForDataPropagation();

        try (PreparedStatement statement = getConnection().prepareStatement("SELECT * FROM facebook.favorite_quotes WHERE user_id = ?")) {
            statement.setObject(1, facebookUser.getId());
            assertTrue(statement.execute());
            ResultSet resultSet = statement.getResultSet();
            assertFalse(resultSet.next());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}