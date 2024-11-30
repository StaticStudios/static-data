package net.staticstudios.data.test;

import net.staticstudios.data.mocks.facebook.MockFacebookFriendEntry;
import net.staticstudios.data.mocks.facebook.MockFacebookService;
import net.staticstudios.data.mocks.facebook.MockFacebookUser;
import net.staticstudios.data.util.DataTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junitpioneer.jupiter.RetryingTest;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

public class PersistentCollectionTest extends DataTest {
    static int NUM_INSTANCES = 3;
    static int NUM_USERS = 10;
    static int NUM_FRIENDS = 5;

    List<MockFacebookService> facebookServices = new ArrayList<>();
    List<UUID> userIds = new ArrayList<>();

    @AfterAll
    static void teardown() throws IOException {
        postgres.stop();
        redis.stop();
    }

    @BeforeEach
    void setup() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("CREATE SCHEMA IF NOT EXISTS facebook");

            statement.execute("""
                    CREATE TABLE facebook.users (
                        id uuid PRIMARY KEY
                    )
                    """);

            statement.execute("""
                    CREATE TABLE facebook.friends (
                        id uuid,
                        friend_id uuid,
                        favorite boolean,
                        PRIMARY KEY (id, friend_id)
                    )
                    """);

            //Insert some data
            for (int i = 0; i < NUM_USERS; i++) {
                UUID userId = UUID.randomUUID();
                userIds.add(userId);
                statement.execute("INSERT INTO facebook.users (id) VALUES ('" + userId + "')");

                for (int j = 0; j < NUM_FRIENDS; j++) {
                    UUID friendId = UUID.randomUUID();
                    statement.execute("INSERT INTO facebook.users (id) VALUES ('" + friendId + "')");
                    statement.execute("INSERT INTO facebook.friends (id, friend_id, favorite) VALUES ('" + userId + "', '" + friendId + "', false)");
                }
            }

            UUID sessionId = UUID.randomUUID();

            for (int i = 0; i < NUM_INSTANCES; i++) {
                facebookServices.add(new MockFacebookService(sessionId + "-facebook-service-" + i, redis.getHost(), redis.getBindPort(), hikariConfig));
            }
        }
    }

    @AfterEach
    void cleanup() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("DROP SCHEMA IF EXISTS facebook CASCADE");
        }
    }

    @RetryingTest(maxAttempts = 5, suspendForMs = 100)
    @DisplayName("Ensure entries are loaded properly")
    void testDataLoad() {
        assertAll("friends",
                facebookServices.stream().map(service -> () -> {
                    MockFacebookUser user = service.getUserProvider().get(userIds.getFirst());
                    assertEquals(NUM_FRIENDS, user.getFriends().size());
                }));
    }

    @RetryingTest(maxAttempts = 5, suspendForMs = 100)
    @DisplayName("Ensure entries are added and removed from all instances")
    void testPropagation() {
        MockFacebookService facebookService0 = facebookServices.getFirst();
        MockFacebookUser user0 = facebookService0.getUserProvider().createUser(List.of(
                new MockFacebookFriendEntry(facebookService0.getDataManager(), UUID.randomUUID()),
                new MockFacebookFriendEntry(facebookService0.getDataManager(), UUID.randomUUID())
        ));

        waitForDataPropagation();

        assertAll("friends",
                facebookServices.stream().map(service -> () -> {
                    MockFacebookUser user = service.getUserProvider().get(user0.getId());
                    assertEquals(2, user.getFriends().size());
                }));

        //Ensure that no update, add, or remove handler was called
        assertAll("handlers",
                facebookServices.stream().map(service -> () -> {
                    MockFacebookUser user = service.getUserProvider().get(user0.getId());
                    assertNull(user.getLastFriendUpdated());
                    assertNull(user.getLastFriendAdded());
                    assertNull(user.getLastFriendRemoved());
                }));

        MockFacebookFriendEntry friend = user0.getFriends().stream().findFirst().orElse(null);
        assertNotNull(friend);

        friend.setFavorite(true);
        friend.update(user0.getFriends());

        //Ensure the local update handler was called instantly
        assertEquals(friend, user0.getLastFriendUpdated());

        waitForDataPropagation();

        //Ensure the update handler was called on all instances
        assertAll("handlers",
                facebookServices.stream().map(service -> () -> {
                    MockFacebookUser user = service.getUserProvider().get(user0.getId());
                    assertEquals(friend, user.getLastFriendUpdated());
                }));

        user0.removeFriend(friend.getId());
        //Ensure the local remove handler was called instantly
        assertEquals(friend, user0.getLastFriendRemoved());

        waitForDataPropagation();

        //Ensure the remove handler was called on all instances
        assertAll("handlers",
                facebookServices.stream().map(service -> () -> {
                    MockFacebookUser user = service.getUserProvider().get(user0.getId());
                    assertEquals(friend, user.getLastFriendRemoved());
                }));

        UUID newFriendId = UUID.randomUUID();
        user0.addFriend(newFriendId);
        //Ensure the local add handler was called instantly
        assertEquals(newFriendId, user0.getLastFriendAdded().getId());

        waitForDataPropagation();

        //Ensure the add handler was called on all instances
        assertAll("handlers",
                facebookServices.stream().map(service -> () -> {
                    MockFacebookUser user = service.getUserProvider().get(user0.getId());
                    assertEquals(newFriendId, user.getLastFriendAdded().getId());
                }));
    }
}