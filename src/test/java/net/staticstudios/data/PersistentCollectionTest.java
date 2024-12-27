package net.staticstudios.data;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
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
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

public class PersistentCollectionTest extends DataTest {

    //todo: we need to add add handlers add and remove handlers
    //todo: we need to test what happens when we manually edit the db. do the collections update?
    //todo: test blocking #add, #addAll, ... calls

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
                    create table if not exists facebook.user_following (
                                                              user_id uuid,
                                                              following_id uuid
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

    @RetryingTest(5)
    public void testLoadingUniqueDataCollection() {
        List<UUID> ids = new ArrayList<>();
        try (Statement statement = getConnection().createStatement()) {
            for (int i = 0; i < 10; i++) {
                UUID id = UUID.randomUUID();
                ids.add(id);
                statement.executeUpdate("insert into facebook.users (id) values ('" + id + "')");
                statement.executeUpdate("insert into facebook.posts (id, user_id, description) values ('" + UUID.randomUUID() + "', '" + id + "', 'post - " + id + "')");
                statement.executeUpdate("insert into facebook.posts (id, user_id, description) values ('" + UUID.randomUUID() + "', '" + id + "', 'post 2 - " + id + "')");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        MockEnvironment environment = createMockEnvironment();
        getMockEnvironments().add(environment);

        DataManager dataManager = environment.dataManager();
        dataManager.loadAll(FacebookUser.class);

        for (UUID id : ids) {
            FacebookUser user = dataManager.get(FacebookUser.class, id);
            assertEquals(2, user.getPosts().size());
            assertTrue(user.getPosts().stream().anyMatch(post -> post.getDescription().equals("post - " + id)));
            assertTrue(user.getPosts().stream().anyMatch(post -> post.getDescription().equals("post 2 - " + id)));
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

    @RetryingTest(5)
    public void testLoadingValueCollection() {
        List<UUID> ids = new ArrayList<>();
        try (Statement statement = getConnection().createStatement()) {
            for (int i = 0; i < 10; i++) {
                UUID id = UUID.randomUUID();
                ids.add(id);
                statement.executeUpdate("insert into facebook.users (id) values ('" + id + "')");
                statement.executeUpdate("insert into facebook.favorite_quotes (id, user_id, quote) values ('" + UUID.randomUUID() + "', '" + id + "', 'quote - " + id + "')");
                statement.executeUpdate("insert into facebook.favorite_quotes (id, user_id, quote) values ('" + UUID.randomUUID() + "', '" + id + "', 'quote 2 - " + id + "')");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        MockEnvironment environment = createMockEnvironment();
        getMockEnvironments().add(environment);

        DataManager dataManager = environment.dataManager();
        dataManager.loadAll(FacebookUser.class);

        for (UUID id : ids) {
            FacebookUser user = dataManager.get(FacebookUser.class, id);
            assertEquals(2, user.getFavoriteQuotes().size());
            assertTrue(user.getFavoriteQuotes().contains("quote - " + id));
            assertTrue(user.getFavoriteQuotes().contains("quote 2 - " + id));
        }
    }

    @RetryingTest(5)
    public void testAddToManyToManyCollection() {
        MockEnvironment mockEnvironment = getMockEnvironments().getFirst();
        DataManager dataManager = mockEnvironment.dataManager();

        FacebookUser facebookUser = FacebookUser.createSync(dataManager);

        FacebookUser following1 = FacebookUser.createSync(dataManager);
        FacebookUser following2 = FacebookUser.createSync(dataManager);

        facebookUser.getFollowing().add(following1);
        assertEquals(1, facebookUser.getFollowing().size());
        facebookUser.getFollowing().add(following2);
        assertEquals(2, facebookUser.getFollowing().size());

        assertTrue(facebookUser.getFollowing().contains(following1));
        assertTrue(facebookUser.getFollowing().contains(following2));

        assertTrue(following1.getFollowers().contains(facebookUser));
        assertTrue(following2.getFollowers().contains(facebookUser));

        waitForDataPropagation();

        try (PreparedStatement statement = getConnection().prepareStatement("SELECT * FROM facebook.user_following WHERE user_id = ?")) {
            statement.setObject(1, facebookUser.getId());
            assertTrue(statement.execute());
            List<UUID> followingIds = new ArrayList<>();
            ResultSet resultSet = statement.getResultSet();
            while (resultSet.next()) {
                followingIds.add(resultSet.getObject("following_id", UUID.class));
            }

            assertEquals(2, followingIds.size());
            assertTrue(followingIds.contains(following1.getId()));
            assertTrue(followingIds.contains(following2.getId()));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


    @RetryingTest(5)
    public void testAddAllToManyToManyCollection() {
        MockEnvironment mockEnvironment = getMockEnvironments().getFirst();
        DataManager dataManager = mockEnvironment.dataManager();

        FacebookUser facebookUser = FacebookUser.createSync(dataManager);

        FacebookUser following1 = FacebookUser.createSync(dataManager);
        FacebookUser following2 = FacebookUser.createSync(dataManager);

        facebookUser.getFollowing().addAll(List.of(following1, following2));
        assertEquals(2, facebookUser.getFollowing().size());

        assertTrue(facebookUser.getFollowing().contains(following1));
        assertTrue(facebookUser.getFollowing().contains(following2));

        assertTrue(following1.getFollowers().contains(facebookUser));
        assertTrue(following2.getFollowers().contains(facebookUser));

        waitForDataPropagation();

        try (PreparedStatement statement = getConnection().prepareStatement("SELECT * FROM facebook.user_following WHERE user_id = ?")) {
            statement.setObject(1, facebookUser.getId());
            assertTrue(statement.execute());
            List<UUID> followingIds = new ArrayList<>();
            ResultSet resultSet = statement.getResultSet();
            while (resultSet.next()) {
                followingIds.add(resultSet.getObject("following_id", UUID.class));
            }

            assertEquals(2, followingIds.size());
            assertTrue(followingIds.contains(following1.getId()));
            assertTrue(followingIds.contains(following2.getId()));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @RetryingTest(5)
    public void testRemoveFromManyToManyCollection() {
        MockEnvironment mockEnvironment = getMockEnvironments().getFirst();
        DataManager dataManager = mockEnvironment.dataManager();

        FacebookUser facebookUser = FacebookUser.createSync(dataManager);

        FacebookUser following1 = FacebookUser.createSync(dataManager);
        FacebookUser following2 = FacebookUser.createSync(dataManager);

        facebookUser.getFollowing().add(following1);
        facebookUser.getFollowing().add(following2);

        assertEquals(2, facebookUser.getFollowing().size());
        assertTrue(facebookUser.getFollowing().contains(following1));
        assertTrue(facebookUser.getFollowing().contains(following2));

        assertTrue(facebookUser.getFollowing().remove(following1));

        assertEquals(1, facebookUser.getFollowing().size());
        assertFalse(facebookUser.getFollowing().contains(following1));
        assertTrue(facebookUser.getFollowing().contains(following2));

        assertFalse(following1.getFollowers().contains(facebookUser));
        assertTrue(following2.getFollowers().contains(facebookUser));

        waitForDataPropagation();

        try (PreparedStatement statement = getConnection().prepareStatement("SELECT * FROM facebook.user_following WHERE user_id = ?")) {
            statement.setObject(1, facebookUser.getId());
            assertTrue(statement.execute());
            List<UUID> followingIds = new ArrayList<>();
            ResultSet resultSet = statement.getResultSet();
            while (resultSet.next()) {
                followingIds.add(resultSet.getObject("following_id", UUID.class));
            }

            assertEquals(1, followingIds.size());
            assertFalse(followingIds.contains(following1.getId()));
            assertTrue(followingIds.contains(following2.getId()));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @RetryingTest(5)
    public void testRemoveAllFromManyToManyCollection() {
        MockEnvironment mockEnvironment = getMockEnvironments().getFirst();
        DataManager dataManager = mockEnvironment.dataManager();

        FacebookUser facebookUser = FacebookUser.createSync(dataManager);

        FacebookUser following1 = FacebookUser.createSync(dataManager);
        FacebookUser following2 = FacebookUser.createSync(dataManager);
        FacebookUser following3 = FacebookUser.createSync(dataManager);

        facebookUser.getFollowing().add(following1);
        facebookUser.getFollowing().add(following2);
        facebookUser.getFollowing().add(following3);

        assertEquals(3, facebookUser.getFollowing().size());
        assertTrue(facebookUser.getFollowing().contains(following1));
        assertTrue(facebookUser.getFollowing().contains(following2));
        assertTrue(facebookUser.getFollowing().contains(following3));

        assertTrue(facebookUser.getFollowing().removeAll(List.of(following1, following2)));

        assertEquals(1, facebookUser.getFollowing().size());
        assertFalse(facebookUser.getFollowing().contains(following1));
        assertFalse(facebookUser.getFollowing().contains(following2));
        assertTrue(facebookUser.getFollowing().contains(following3));

        assertFalse(following1.getFollowers().contains(facebookUser));
        assertFalse(following2.getFollowers().contains(facebookUser));
        assertTrue(following3.getFollowers().contains(facebookUser));

        waitForDataPropagation();

        try (PreparedStatement statement = getConnection().prepareStatement("SELECT * FROM facebook.user_following WHERE user_id = ?")) {
            statement.setObject(1, facebookUser.getId());
            assertTrue(statement.execute());
            List<UUID> followingIds = new ArrayList<>();
            ResultSet resultSet = statement.getResultSet();
            while (resultSet.next()) {
                followingIds.add(resultSet.getObject("following_id", UUID.class));
            }

            assertEquals(1, followingIds.size());
            assertFalse(followingIds.contains(following1.getId()));
            assertFalse(followingIds.contains(following2.getId()));
            assertTrue(followingIds.contains(following3.getId()));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @RetryingTest(5)
    public void testContainsInManyToManyCollection() {
        MockEnvironment mockEnvironment = getMockEnvironments().getFirst();
        DataManager dataManager = mockEnvironment.dataManager();

        FacebookUser facebookUser1 = FacebookUser.createSync(dataManager);
        FacebookUser facebookUser2 = FacebookUser.createSync(dataManager);

        FacebookUser following1 = FacebookUser.createSync(dataManager);
        FacebookUser following2 = FacebookUser.createSync(dataManager);

        facebookUser1.getFollowing().add(following1);
        facebookUser1.getFollowing().add(following2);

        facebookUser2.getFollowing().add(following1);

        assertTrue(facebookUser1.getFollowing().contains(following1));
        assertTrue(facebookUser1.getFollowing().contains(following2));
        assertTrue(facebookUser2.getFollowing().contains(following1));
        assertFalse(facebookUser2.getFollowing().contains(following2));
        assertFalse(facebookUser1.getFollowing().contains(facebookUser2));
        assertFalse(facebookUser1.getFollowing().contains("Not a user"));
        assertFalse(facebookUser1.getFollowing().contains(null));
    }

    @RetryingTest(5)
    public void testContainsAllInManyToManyCollection() {
        MockEnvironment mockEnvironment = getMockEnvironments().getFirst();
        DataManager dataManager = mockEnvironment.dataManager();

        FacebookUser facebookUser1 = FacebookUser.createSync(dataManager);
        FacebookUser facebookUser2 = FacebookUser.createSync(dataManager);

        FacebookUser following1 = FacebookUser.createSync(dataManager);
        FacebookUser following2 = FacebookUser.createSync(dataManager);

        facebookUser1.getFollowing().add(following1);
        facebookUser1.getFollowing().add(following2);

        facebookUser2.getFollowing().add(following1);

        assertTrue(facebookUser1.getFollowing().contains(following1));
        assertTrue(facebookUser1.getFollowing().contains(following2));
        assertTrue(facebookUser2.getFollowing().contains(following1));
        assertFalse(facebookUser2.getFollowing().contains(following2));

        assertTrue(facebookUser1.getFollowing().containsAll(List.of(following1, following2)));
        assertFalse(facebookUser1.getFollowing().containsAll(List.of(following1, following2, "Not a user")));
        List<Object> bad = new ArrayList<>();
        bad.add(following1);
        bad.add(following2);
        bad.add(null);
        assertFalse(facebookUser1.getFollowing().containsAll(bad));
        assertTrue(facebookUser1.getFollowing().containsAll(List.of()));
    }

    @RetryingTest(5)
    public void testRetainAllInManyToManyCollection() {
        MockEnvironment mockEnvironment = getMockEnvironments().getFirst();
        DataManager dataManager = mockEnvironment.dataManager();

        FacebookUser facebookUser1 = FacebookUser.createSync(dataManager);

        FacebookUser following1 = FacebookUser.createSync(dataManager);
        FacebookUser following2 = FacebookUser.createSync(dataManager);
        FacebookUser following3 = FacebookUser.createSync(dataManager);

        facebookUser1.getFollowing().add(following1);
        facebookUser1.getFollowing().add(following2);
        facebookUser1.getFollowing().add(following3);

        assertEquals(3, facebookUser1.getFollowing().size());

        assertTrue(facebookUser1.getFollowing().retainAll(List.of(following1, following2)));

        assertEquals(2, facebookUser1.getFollowing().size());
        assertTrue(facebookUser1.getFollowing().contains(following1));
        assertTrue(facebookUser1.getFollowing().contains(following2));
        assertFalse(facebookUser1.getFollowing().contains(following3));

        assertTrue(facebookUser1.getFollowing().retainAll(List.of(following1)));

        assertEquals(1, facebookUser1.getFollowing().size());
        assertTrue(facebookUser1.getFollowing().contains(following1));
        assertFalse(facebookUser1.getFollowing().contains(following2));
        assertFalse(facebookUser1.getFollowing().contains(following3));
    }

    @RetryingTest(5)
    public void testToArrayInManyToManyCollection() {
        MockEnvironment mockEnvironment = getMockEnvironments().getFirst();
        DataManager dataManager = mockEnvironment.dataManager();

        FacebookUser facebookUser1 = FacebookUser.createSync(dataManager);

        FacebookUser following1 = FacebookUser.createSync(dataManager);
        FacebookUser following2 = FacebookUser.createSync(dataManager);

        facebookUser1.getFollowing().add(following1);
        facebookUser1.getFollowing().add(following2);

        assertEquals(2, facebookUser1.getFollowing().size());

        FacebookUser[] followings = facebookUser1.getFollowing().toArray(new FacebookUser[0]);
        assertEquals(2, followings.length);
        boolean containsFollowing1 = false;
        boolean containsFollowing2 = false;

        for (FacebookUser following : followings) {
            if (following.equals(following1)) {
                containsFollowing1 = true;
            } else if (following.equals(following2)) {
                containsFollowing2 = true;
            }
        }

        assertTrue(containsFollowing1);
        assertTrue(containsFollowing2);

        Object[] objects = facebookUser1.getFollowing().toArray();
        assertEquals(2, objects.length);
        boolean containsFollowing1Object = false;
        boolean containsFollowing2Object = false;

        for (Object object : objects) {
            if (object.equals(following1)) {
                containsFollowing1Object = true;
            } else if (object.equals(following2)) {
                containsFollowing2Object = true;
            }
        }

        assertTrue(containsFollowing1Object);
        assertTrue(containsFollowing2Object);
    }

    @RetryingTest(5)
    public void testIteratorInManyToManyCollection() {
        MockEnvironment mockEnvironment = getMockEnvironments().getFirst();
        DataManager dataManager = mockEnvironment.dataManager();

        FacebookUser facebookUser1 = FacebookUser.createSync(dataManager);

        FacebookUser following1 = FacebookUser.createSync(dataManager);
        FacebookUser following2 = FacebookUser.createSync(dataManager);

        facebookUser1.getFollowing().add(following1);
        facebookUser1.getFollowing().add(following2);

        assertEquals(2, facebookUser1.getFollowing().size());

        Iterator<FacebookUser> iterator = facebookUser1.getFollowing().iterator();
        assertTrue(iterator.hasNext());
        FacebookUser next = iterator.next();
        assertTrue(next.equals(following1) || next.equals(following2));
        assertTrue(iterator.hasNext());
        next = iterator.next();
        assertTrue(next.equals(following1) || next.equals(following2));
        assertFalse(iterator.hasNext());

        iterator = facebookUser1.getFollowing().iterator();
        assertTrue(iterator.hasNext());
        iterator.next();
        iterator.remove();

        assertEquals(1, facebookUser1.getFollowing().size());

        assertTrue(iterator.hasNext());
        iterator.next();
        iterator.remove();

        assertEquals(0, facebookUser1.getFollowing().size());

        assertFalse(iterator.hasNext());

        waitForDataPropagation();

        try (PreparedStatement statement = getConnection().prepareStatement("SELECT * FROM facebook.user_following WHERE user_id = ?")) {
            statement.setObject(1, facebookUser1.getId());
            assertTrue(statement.execute());
            ResultSet resultSet = statement.getResultSet();
            assertFalse(resultSet.next());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @RetryingTest(5)
    public void testLoadingManyToManyCollection() {
        Multimap<UUID, UUID> followingMap = HashMultimap.create();
        try (Statement statement = getConnection().createStatement()) {
            for (int i = 0; i < 10; i++) {
                UUID userId = UUID.randomUUID();
                UUID followingId1 = UUID.randomUUID();
                UUID followingId2 = UUID.randomUUID();
                followingMap.put(userId, followingId1);
                followingMap.put(userId, followingId2);
                statement.executeUpdate("insert into facebook.users (id) values ('" + userId + "')");
                statement.executeUpdate("insert into facebook.users (id) values ('" + followingId1 + "')");
                statement.executeUpdate("insert into facebook.users (id) values ('" + followingId2 + "')");
                statement.executeUpdate("insert into facebook.user_following (user_id, following_id) values ('" + userId + "', '" + followingId1 + "')");
                statement.executeUpdate("insert into facebook.user_following (user_id, following_id) values ('" + userId + "', '" + followingId2 + "')");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        MockEnvironment environment = createMockEnvironment();
        getMockEnvironments().add(environment);

        DataManager dataManager = environment.dataManager();
        dataManager.loadAll(FacebookUser.class);

        for (UUID userId : followingMap.keySet()) {
            FacebookUser user = dataManager.get(FacebookUser.class, userId);
            assertEquals(2, user.getFollowing().size());
            assertTrue(user.getFollowing().stream().anyMatch(following -> followingMap.get(userId).contains(following.getId())));
        }
    }

    //todo: test:
    // blocking add - ALL PCs
    // blocking addAll - ALL PCs
    // blocking remove - ALL PCs
    // blocking removeAll - ALL PCs
    // clear - ALL PCs
    // blocking clear - ALL PCs
    // blocking iterator - ALL PCs


}