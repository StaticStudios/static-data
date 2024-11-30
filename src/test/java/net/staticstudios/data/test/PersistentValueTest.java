package net.staticstudios.data.test;

import net.staticstudios.data.UpdatedValue;
import net.staticstudios.data.mocks.spotify.MockSpotifyService;
import net.staticstudios.data.mocks.spotify.MockSpotifyUser;
import net.staticstudios.data.util.DataTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junitpioneer.jupiter.RetryingTest;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

public class PersistentValueTest extends DataTest {
    static int NUM_INSTANCES = 3;
    static int NUM_USERS = 10;

    List<MockSpotifyService> spotifyServices = new ArrayList<>();
    List<UUID> userIds = new ArrayList<>();

    @AfterAll
    static void teardown() throws IOException {
        postgres.stop();
        redis.stop();
    }

    @BeforeEach
    void setup() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("CREATE SCHEMA IF NOT EXISTS spotify");

            statement.execute("""
                    CREATE TABLE spotify.users (
                        id uuid PRIMARY KEY,
                        name TEXT NOT NULL,
                        minutes_listened integer NOT NULL DEFAULT 0,
                        created_at timestamp NOT NULL DEFAULT now()
                    )
                    """);


            //Insert some data
            for (int i = 0; i < NUM_USERS; i++) {
                UUID userId = UUID.randomUUID();
                userIds.add(userId);
                statement.execute("INSERT INTO spotify.users (id, name) VALUES ('" + userId + "', 'User" + i + "')");
            }
        }

        UUID sessionId = UUID.randomUUID();

        for (int i = 0; i < NUM_INSTANCES; i++) {
            spotifyServices.add(new MockSpotifyService(sessionId + "-spotify-service-" + i, redis.getHost(), redis.getBindPort(), hikariConfig));
        }
    }

    @AfterEach
    void cleanup() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("DROP SCHEMA IF EXISTS spotify CASCADE");
        }
    }

    @RetryingTest(maxAttempts = 5, suspendForMs = 100)
    @DisplayName("Create a new user and ensure that the default values are present")
    void testDefaults() {
        MockSpotifyService spotifyService = spotifyServices.getFirst();
        MockSpotifyUser user0 = spotifyService.getUserProvider().createUser("TestUser");

        assertNotNull(user0);
        assertEquals("TestUser", user0.getName());
        assertEquals(0, user0.getMinutesListened());
        assertNotNull(user0.getCreatedAt());

        waitForDataPropagation();

        assertAll("defaults",
                spotifyServices.stream().map(service -> () -> {
                    MockSpotifyUser user = service.getUserProvider().get(user0.getId());
                    assertNotNull(user);
                    assertEquals("TestUser", user.getName());
                    assertEquals(0, user.getMinutesListened());
                    assertNotNull(user.getCreatedAt());
                }));
    }

    @RetryingTest(maxAttempts = 5, suspendForMs = 100)
    @DisplayName("Change a user's persistent values and ensure that they are updated")
    void testPropagation() {
        MockSpotifyService spotifyService0 = spotifyServices.getFirst();
        MockSpotifyUser user0 = spotifyService0.getUserProvider().get(userIds.getFirst());

        //The value should be updated instantly on the local instance
        user0.setMinutesListened(100);
        assertEquals(100, user0.getMinutesListened());

        waitForDataPropagation();

        //The value should be updated on the other instance
        assertAll("minutesListened",
                spotifyServices.stream().map(service -> () -> {
                    MockSpotifyUser user = service.getUserProvider().get(userIds.getFirst());
                    assertEquals(100, user.getMinutesListened());
                }));
    }

    @RetryingTest(maxAttempts = 5, suspendForMs = 100)
    @DisplayName("Change a user's persistent values and ensure that the update handler is called on every instance")
    void testUpdateHandler() {
        MockSpotifyService spotifyService0 = spotifyServices.getFirst();
        MockSpotifyUser user0 = spotifyService0.getUserProvider().get(userIds.getFirst());

        assertAll("no update handler called",
                spotifyServices.stream().map(service -> () -> {
                    MockSpotifyUser user = service.getUserProvider().get(userIds.getFirst());
                    assertNull(user.getLastMinutesListenedUpdate());
                    assertNull(user.getLastCreatedAtUpdate());
                    assertNull(user.getLastNameUpdate());
                }));

        int originalMinutesListened = user0.getMinutesListened();
        int updatedMinutesListened = 100;

        //Update the minutes listened, and ensure that the update handler was called instantly
        user0.setMinutesListened(updatedMinutesListened);
        assertEquals(UpdatedValue.of(originalMinutesListened, updatedMinutesListened), user0.getLastMinutesListenedUpdate());

        waitForDataPropagation();

        assertAll("minutes listened update handler",
                spotifyServices.stream().map(service -> () -> {
                    UpdatedValue<Integer> expected = UpdatedValue.of(originalMinutesListened, updatedMinutesListened);
                    MockSpotifyUser user = service.getUserProvider().get(userIds.getFirst());
                    assertEquals(expected, user.getLastMinutesListenedUpdate(), service.getDataManager().getServerId());
                }));

        Timestamp originalCreatedAt = user0.getCreatedAt();
        Timestamp updatedCreatedAt = Timestamp.from(Instant.now());

        user0.setCreatedAt(updatedCreatedAt);

        waitForDataPropagation();

        assertAll("created at update handler",
                spotifyServices.stream().map(service -> () -> {
                    UpdatedValue<Timestamp> expected = UpdatedValue.of(originalCreatedAt, updatedCreatedAt);
                    MockSpotifyUser user = service.getUserProvider().get(userIds.getFirst());
                    assertEquals(expected, user.getLastCreatedAtUpdate(), service.getDataManager().getServerId());
                }));

        String originalName = user0.getName();
        String updatedName = "UpdatedName";

        user0.setName(updatedName);

        waitForDataPropagation();

        assertAll("name update handler",
                spotifyServices.stream().map(service -> () -> {
                    UpdatedValue<String> expected = UpdatedValue.of(originalName, updatedName);
                    MockSpotifyUser user = service.getUserProvider().get(userIds.getFirst());
                    assertEquals(expected, user.getLastNameUpdate(), service.getDataManager().getServerId());
                }));
    }

    //todo: query the db and ensure the results are correct
}