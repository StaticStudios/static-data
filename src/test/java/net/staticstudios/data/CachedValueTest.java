package net.staticstudios.data;

import net.staticstudios.data.misc.DataTest;
import net.staticstudios.data.misc.MockEnvironment;
import net.staticstudios.data.mock.cachedvalue.RedditUser;
import net.staticstudios.data.primative.Primitives;
import org.junit.jupiter.api.BeforeEach;
import org.junitpioneer.jupiter.RetryingTest;
import redis.clients.jedis.Jedis;

import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

public class CachedValueTest extends DataTest {
    //todo: test loading from redis

    @BeforeEach
    public void init() {
        try (Statement statement = getConnection().createStatement()) {
            statement.executeUpdate("""
                    drop schema if exists reddit cascade;
                    create schema if not exists reddit;
                    create table if not exists reddit.users (
                        id uuid primary key
                    );
                    """);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        getMockEnvironments().forEach(env -> {
            DataManager dataManager = env.dataManager();
            dataManager.loadAll(RedditUser.class);
        });
    }

    @RetryingTest(5)
    public void testSetCachedValue() {
        MockEnvironment environment = getMockEnvironments().getFirst();
        DataManager dataManager = environment.dataManager();

        RedditUser user = RedditUser.createSync(dataManager);

        Jedis jedis = getJedis();

        assertFalse(jedis.exists(user.status.getKey().toString())); //it's null by default
        assertFalse(jedis.exists(user.lastLogin.getKey().toString())); //it's null by default

        user.setStatus("Hello, World!");
        assertEquals("Hello, World!", user.getStatus());

        user.setLastLogin(Timestamp.from(Instant.EPOCH));
        assertEquals(Timestamp.from(Instant.EPOCH), user.getLastLogin());

        waitForDataPropagation();

        assertEquals("Hello, World!", jedis.get(user.status.getKey().toString()));
        assertEquals(Primitives.encode(Timestamp.from(Instant.EPOCH)), jedis.get(user.lastLogin.getKey().toString()));

        user.setStatus("Goodbye, World!");
        assertEquals("Goodbye, World!", user.getStatus());

        waitForDataPropagation();

        assertEquals("Goodbye, World!", jedis.get(user.status.getKey().toString()));

        jedis.del(user.status.getKey().toString());

        waitForDataPropagation();

        assertNull(user.getStatus());

        jedis.set(user.status.getKey().toString(), Primitives.encode("Hello, World!"));

        waitForDataPropagation();

        assertEquals("Hello, World!", user.getStatus());
    }

    @RetryingTest(5)
    public void testExpiringCachedValue() {
        MockEnvironment environment = getMockEnvironments().getFirst();
        DataManager dataManager = environment.dataManager();

        RedditUser user = RedditUser.createSync(dataManager);

        Jedis jedis = getJedis();

        assertFalse(jedis.exists(user.suspended.getKey().toString()));

        user.setSuspended(true);
        assertTrue(user.isSuspended());

        waitForDataPropagation();

        assertTrue(jedis.exists(user.suspended.getKey().toString()));

        //wait for the key to expire
        try {
            Thread.sleep(4000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        assertFalse(jedis.exists(user.suspended.getKey().toString()));
        assertFalse(user.isSuspended());
    }

    @RetryingTest(5)
    public void testFallbackValue() {
        MockEnvironment environment = getMockEnvironments().getFirst();
        DataManager dataManager = environment.dataManager();

        RedditUser user = RedditUser.createSync(dataManager);

        Jedis jedis = getJedis();

        assertFalse(jedis.exists(user.bio.getKey().toString()));

        assertEquals("This user has not set a bio yet.", user.getBio());

        user.setBio("Hello, World!");
        assertEquals("Hello, World!", user.getBio());

        waitForDataPropagation();

        assertEquals("Hello, World!", jedis.get(user.bio.getKey().toString()));

        jedis.del(user.bio.getKey().toString());

        waitForDataPropagation();

        assertEquals("This user has not set a bio yet.", user.getBio());

        user.setBio("Goodbye, World!");
        assertEquals("Goodbye, World!", user.getBio());

        user.setBio(null);
        assertEquals("This user has not set a bio yet.", user.getBio());
    }

    @RetryingTest(5)
    public void testUpdateHandler() {
        MockEnvironment environment = getMockEnvironments().getFirst();
        DataManager dataManager = environment.dataManager();

        RedditUser user = RedditUser.createSync(dataManager);

        Jedis jedis = getJedis();

        assertEquals(0, user.getStatusUpdates());

        user.setStatus("Hello, World!");
        assertEquals(1, user.getStatusUpdates());

        // Unlike persistent values, we can't ignore updates from ourselves.
        // However, the update handler won't be called when we get the notification from redis if the value is the same.
        // So that's why we wait, to skip that additional call.
        waitForDataPropagation();

        user.setStatus("Goodbye, World!");
        assertEquals(2, user.getStatusUpdates());

        waitForDataPropagation();

        jedis.set(user.status.getKey().toString(), Primitives.encode("Hello, World!"));

        waitForDataPropagation();

        assertEquals(3, user.getStatusUpdates());
    }
}