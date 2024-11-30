package net.staticstudios.data.test;

import net.staticstudios.data.DeletionType;
import net.staticstudios.data.UpdatedValue;
import net.staticstudios.data.mocks.reddit.MockRedditService;
import net.staticstudios.data.mocks.reddit.MockRedditUser;
import net.staticstudios.data.util.DataTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junitpioneer.jupiter.RetryingTest;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

public class CachedValueTest extends DataTest {
    static int NUM_INSTANCES = 3;
    static int NUM_USERS = 10;

    List<MockRedditService> redditServices = new ArrayList<>();
    List<UUID> userIds = new ArrayList<>();

    @AfterAll
    static void teardown() throws IOException {
        postgres.stop();
        redis.stop();
    }

    @BeforeEach
    void setup() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("CREATE SCHEMA IF NOT EXISTS reddit");

            statement.execute("""
                    CREATE TABLE reddit.users (
                        id uuid PRIMARY KEY
                    )
                    """);


            //Insert some data
            for (int i = 0; i < NUM_USERS; i++) {
                UUID userId = UUID.randomUUID();
                userIds.add(userId);
                statement.execute("INSERT INTO reddit.users (id) VALUES ('" + userId + "')");
            }
        }

        UUID sessionId = UUID.randomUUID();

        for (int i = 0; i < NUM_INSTANCES; i++) {
            redditServices.add(new MockRedditService(sessionId + "-reddit-service-" + i, redis.getHost(), redis.getBindPort(), hikariConfig));
        }
    }

    @AfterEach
    void cleanup() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("DROP SCHEMA IF EXISTS reddit CASCADE");
        }
    }

    @RetryingTest(maxAttempts = 5, suspendForMs = 100)
    @DisplayName("Change a user's online status")
    void testUpdatingCachedValues() {
        MockRedditService service0 = redditServices.getFirst();
        MockRedditUser user0 = service0.getUserProvider().get(userIds.getFirst());

        //Users are offline by default, so nothing should exist in redis yet
        String key = user0.getDataManager().buildCachedValueKey("is_online", "reddit.users", user0.getId());
        try (Jedis jedis = service0.getDataManager().getJedis()) {
            assertFalse(jedis.exists(key));
        }

        //Validate that no update handler has been called yet
        assertAll("validate update handler", redditServices.stream().map(service -> () -> {
            MockRedditUser user = service.getUserProvider().get(user0.getId());
            assertNull(user.getLastIsOnlineUpdate());
        }));

        user0.setOnline(true);

        waitForDataPropagation();

        assertAll("validate online status", redditServices.stream().map(service -> () -> {
            MockRedditUser user = service.getUserProvider().get(user0.getId());
            assertTrue(user.isOnline());
        }));

        try (Jedis jedis = service0.getDataManager().getJedis()) {
            assertTrue(jedis.exists(key));
        }

        //Validate that the update handler has been called
        assertAll("validate update handler", redditServices.stream().map(service -> () -> {
            MockRedditUser user = service.getUserProvider().get(user0.getId());
            assertEquals(UpdatedValue.of(false, true), user.getLastIsOnlineUpdate());
        }));

        user0.setOnline(false);

        waitForDataPropagation();

        assertAll("validate online status", redditServices.stream().map(service -> () -> {
            MockRedditUser user = service.getUserProvider().get(user0.getId());
            assertFalse(user.isOnline());
        }));

        //The key should no longer exist since it's set to its default value
        try (Jedis jedis = service0.getDataManager().getJedis()) {
            assertFalse(jedis.exists(key));
        }

        //Validate that the update handler has been called
        assertAll("validate update handler", redditServices.stream().map(service -> () -> {
            MockRedditUser user = service.getUserProvider().get(user0.getId());
            assertEquals(UpdatedValue.of(true, false), user.getLastIsOnlineUpdate());
        }));
    }

    @RetryingTest(maxAttempts = 5, suspendForMs = 100)
    @DisplayName("Delete a user and ensure the redis entry is removed")
    void testDeletingData() {
        MockRedditService service0 = redditServices.getFirst();
        MockRedditUser user0 = service0.getUserProvider().get(userIds.getFirst());
        user0.setOnline(true);

        waitForDataPropagation();

        String key = user0.getDataManager().buildCachedValueKey("is_online", "reddit.users", user0.getId());

        //The key should exist since the user is online
        try (Jedis jedis = service0.getDataManager().getJedis()) {
            assertTrue(jedis.exists(key));
        }

        service0.getUserProvider().delete(user0.getId(), DeletionType.ALL);

        waitForDataPropagation();

        //The key should no longer exist since the user was deleted
        try (Jedis jedis = service0.getDataManager().getJedis()) {
            assertFalse(jedis.exists(key));
        }
    }
}