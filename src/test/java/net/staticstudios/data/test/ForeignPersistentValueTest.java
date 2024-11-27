package net.staticstudios.data.test;

import net.staticstudios.data.UpdatedValue;
import net.staticstudios.data.mocks.discord.MockDiscordService;
import net.staticstudios.data.mocks.discord.MockDiscordUser;
import net.staticstudios.data.mocks.discord.MockDiscordUserStats;
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

public class ForeignPersistentValueTest extends DataTest {
    static int NUM_INSTANCES = 3;
    static int NUM_USERS = 10;

    List<MockDiscordService> discordServices = new ArrayList<>();
    List<UUID> userIds = new ArrayList<>();

    @AfterAll
    static void teardown() throws IOException {
        postgres.stop();
        redis.stop();
    }

    @BeforeEach
    void setup() throws SQLException {

        try (Statement statement = connection.createStatement()) {
            statement.execute("CREATE SCHEMA IF NOT EXISTS discord");

            statement.execute("""
                    CREATE TABLE discord.users (
                        id uuid PRIMARY KEY,
                        name TEXT NOT NULL,
                        favorite_color TEXT NOT NULL DEFAULT 'red'
                    )
                    """);

            statement.execute("""
                    CREATE TABLE discord.stats (
                        id uuid PRIMARY KEY,
                        messages_sent integer NOT NULL DEFAULT 0,
                        favorite_number integer NOT NULL DEFAULT 0
                    )
                    """);

            statement.execute("""
                                CREATE TABLE discord.user_stats (
                                    user_id uuid NOT NULL,
                                    stats_id uuid NOT NULL,
                    PRIMARY KEY (user_id, stats_id)
                                )
                    """);


            //Insert some data
            for (int i = 0; i < NUM_USERS; i++) {
                UUID userId = UUID.randomUUID();
                userIds.add(userId);
                statement.execute("INSERT INTO discord.users (id, name) VALUES ('" + userId + "', 'User" + i + "')");
                statement.execute("INSERT INTO discord.stats (id) VALUES ('" + userId + "')");
            }
        }

        UUID sessionId = UUID.randomUUID();

        for (int i = 0; i < NUM_INSTANCES; i++) {
            discordServices.add(new MockDiscordService(sessionId + "-discord-service-" + i, redis.getHost(), redis.getBindPort(), hikariConfig));
        }
    }

    @AfterEach
    void cleanup() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("DROP SCHEMA IF EXISTS discord CASCADE");
        }
    }

    @RetryingTest(maxAttempts = 5, suspendForMs = 100)
    @DisplayName("Create a new user, create new stats, link the two, and ensure the initial value is correct")
    void insertFPVCheckInitialValue() {
        MockDiscordService service0 = discordServices.getFirst();
        MockDiscordService service1 = discordServices.get(1);

        //Create the user object on service0
        MockDiscordUser user0 = service0.getUserProvider().createUser("TestUser");

        //Create the stats object on service1, and set the initial messages sent to 10
        MockDiscordUserStats stats1 = service1.getUserStatsProvider().createStats();
        stats1.setMessagesSent(10);

        waitForDataPropagation();

        user0.setStatsId(stats1.getId());

        waitForDataPropagation();

        assertEquals(10, user0.getStatsMessagesSent());
        assertEquals("TestUser", stats1.getUserName());
    }

    @RetryingTest(maxAttempts = 5, suspendForMs = 100)
    @DisplayName("Update a foreign persistent value")
    void updateFPV() {
        MockDiscordService service0 = discordServices.getFirst();
        MockDiscordService service1 = discordServices.get(1);

        //Create the user object on service0
        MockDiscordUser user0 = service0.getUserProvider().createUser("TestUser");

        //Create the stats object on service1
        MockDiscordUserStats stats1 = service1.getUserStatsProvider().createStats();

        String userName = user0.getName();
        int initialMessagesSent = 10;
        int favoriteNumber = 22;
        String favoriteColor = "blue";

        stats1.setMessagesSent(initialMessagesSent);
        stats1.setFavoriteNumber(favoriteNumber);
        user0.setFavoriteColor(favoriteColor);

        waitForDataPropagation();

        assertAll("stats messages set",
                discordServices.stream().map(service -> () -> {
                    MockDiscordUserStats stats = service.getUserStatsProvider().get(stats1.getId());
                    assertEquals(initialMessagesSent, stats.getMessagesSent());
                }));

        assertAll("user name not set",
                discordServices.stream().map(service -> () -> {
                    MockDiscordUserStats stats = service.getUserStatsProvider().get(stats1.getId());
                    assertNull(stats.getUserName());
                }));

        assertAll("user favorite number not set",
                discordServices.stream().map(service -> () -> {
                    MockDiscordUser user = service.getUserProvider().get(user0.getId());
                    assertNull(user.getStatsFavoriteNumberFPV().get());
                }));

        assertAll("stats favorite color not set",
                discordServices.stream().map(service -> () -> {
                    MockDiscordUserStats stats = service.getUserStatsProvider().get(stats1.getId());
                    assertNull(stats.getUserFavoriteColorFPV().get());
                }));

        //Link the user to the stats
        user0.setStatsId(stats1.getId());

        waitForDataPropagation();

        assertAll("foreignId set",
                discordServices.stream().map(service -> () -> {
                    MockDiscordUser user = service.getUserProvider().get(user0.getId());
                    MockDiscordUserStats stats = service.getUserStatsProvider().get(stats1.getId());
                    assertEquals(stats1.getId(), user.getStatsId(), service.getDataManager().getServerId());
                    assertEquals(user0.getId(), stats.getUserId(), service.getDataManager().getServerId());
                    assertEquals(stats1.getId(), user.getStatsFavoriteNumberFPV().getForeignObjectId(), service.getDataManager().getServerId());
                    assertEquals(user0.getId(), stats.getUserFavoriteColorFPV().getForeignObjectId(), service.getDataManager().getServerId());
                }));

        /* Ensure that the name is now set on the stats object
         * Despite this being a different FPV entirely, it uses the same linking table,
         * so once one link is established, any other FPV with the same linking table will be updated
         */
        assertAll("stats name set",
                discordServices.stream().map(service -> () -> {
                    MockDiscordUserStats stats = service.getUserStatsProvider().get(stats1.getId());
                    assertEquals(userName, stats.getUserName(), service.getDataManager().getServerId());
                }));

        /* Despite this being a different FPV entirely, it uses the same linking table,
         * so once one link is established, any other FPV with the same linking table will be updated
         */
        assertAll("stats favorite color set",
                discordServices.stream().map(service -> () -> {
                    MockDiscordUserStats stats = service.getUserStatsProvider().get(stats1.getId());
                    assertEquals(favoriteColor, stats.getUserFavoriteColor());
                }));

        /* Despite this being a different FPV entirely, it uses the same linking table,
         * so once one link is established, any other FPV with the same linking table will be updated
         */
        assertAll("user favorite number set",
                discordServices.stream().map(service -> () -> {
                    MockDiscordUser user = service.getUserProvider().get(user0.getId());
                    assertEquals(favoriteNumber, user.getStatsFavoriteNumber());
                }));

        MockDiscordUserStats stats0 = service0.getUserStatsProvider().get(stats1.getId());
        user0.incrementStatsMessagesSent(); //Update the value from an FPV on service0
        assertEquals(initialMessagesSent + 1, stats0.getMessagesSent()); //The PV on service0 should update instantly, since it is local

        waitForDataPropagation();

        //Validate the data actually propagated
        assertAll("stats messages sent updated",
                discordServices.stream().map(service -> () -> {
                    MockDiscordUserStats stats = service.getUserStatsProvider().get(stats1.getId());
                    assertEquals(initialMessagesSent + 1, stats.getMessagesSent());
                }));
    }

    @RetryingTest(maxAttempts = 5, suspendForMs = 100)
    @DisplayName("Unlink a foreign persistent value")
    void unlinkFPV() {
        MockDiscordService service0 = discordServices.getFirst();
        MockDiscordService service1 = discordServices.get(1);

        //Create the user object on service0
        MockDiscordUser user0 = service0.getUserProvider().createUser("TestUser");

        //Create the stats object on service1
        MockDiscordUserStats stats1 = service1.getUserStatsProvider().createStats();

        String userName = user0.getName();
        int initialMessagesSent = 10;
        int favoriteNumber = 22;
        String favoriteColor = "blue";

        stats1.setMessagesSent(initialMessagesSent);
        stats1.setFavoriteNumber(favoriteNumber);
        user0.setFavoriteColor(favoriteColor);

        waitForDataPropagation();

        user0.setStatsId(stats1.getId());

        waitForDataPropagation();

        assertAll("foreignId set",
                discordServices.stream().map(service -> () -> {
                    MockDiscordUser user = service.getUserProvider().get(user0.getId());
                    MockDiscordUserStats stats = service.getUserStatsProvider().get(stats1.getId());
                    assertEquals(stats1.getId(), user.getStatsId(), service.getDataManager().getServerId());
                    assertEquals(user0.getId(), stats.getUserId(), service.getDataManager().getServerId());
                    assertEquals(stats1.getId(), user.getStatsFavoriteNumberFPV().getForeignObjectId(), service.getDataManager().getServerId());
                    assertEquals(user0.getId(), stats.getUserFavoriteColorFPV().getForeignObjectId(), service.getDataManager().getServerId());
                }));

        assertAll("stats name set",
                discordServices.stream().map(service -> () -> {
                    MockDiscordUserStats stats = service.getUserStatsProvider().get(stats1.getId());
                    assertEquals(userName, stats.getUserName(), service.getDataManager().getServerId());
                }));

        assertAll("user messages sent set",
                discordServices.stream().map(service -> () -> {
                    MockDiscordUser user = service.getUserProvider().get(user0.getId());
                    assertEquals(initialMessagesSent, user.getStatsMessagesSent(), service.getDataManager().getServerId());
                }));

        assertAll("stats favorite color set",
                discordServices.stream().map(service -> () -> {
                    MockDiscordUserStats stats = service.getUserStatsProvider().get(stats1.getId());
                    assertEquals(favoriteColor, stats.getUserFavoriteColor());
                }));

        /* Despite this being a different FPV entirely, it uses the same linking table,
         * so once one link is established, any other FPV with the same linking table will be updated
         */
        assertAll("user favorite number set",
                discordServices.stream().map(service -> () -> {
                    MockDiscordUser user = service.getUserProvider().get(user0.getId());
                    assertEquals(favoriteNumber, user.getStatsFavoriteNumber());
                }));

        //Check behavior when unlinking
        user0.setStatsId(null);

        waitForDataPropagation();

        assertAll("foreignId not set",
                discordServices.stream().map(service -> () -> {
                    MockDiscordUser user = service.getUserProvider().get(user0.getId());
                    MockDiscordUserStats stats = service.getUserStatsProvider().get(stats1.getId());
                    assertNull(user.getStatsId(), service.getDataManager().getServerId());
                    assertNull(stats.getUserId(), service.getDataManager().getServerId());
                    assertNull(user.getStatsFavoriteNumberFPV().getForeignObjectId(), service.getDataManager().getServerId());
                    assertNull(stats.getUserFavoriteColorFPV().getForeignObjectId(), service.getDataManager().getServerId());
                }));

        //Validate FPVs are null after unlinking
        assertAll("stats name not set",
                discordServices.stream().map(service -> () -> {
                    MockDiscordUserStats stats = service.getUserStatsProvider().get(stats1.getId());
                    assertNull(stats.getUserName(), service.getDataManager().getServerId());
                }));

        assertAll("stats favorite color not set",
                discordServices.stream().map(service -> () -> {
                    MockDiscordUserStats stats = service.getUserStatsProvider().get(stats1.getId());
                    assertNull(stats.getUserFavoriteColor(), service.getDataManager().getServerId());
                }));

        assertAll("user favorite number not set",
                discordServices.stream().map(service -> () -> {
                    MockDiscordUser user = service.getUserProvider().get(user0.getId());
                    assertEquals(0, user.getStatsFavoriteNumber(), service.getDataManager().getServerId());
                }));

        assertAll("user messages sent not set",
                discordServices.stream().map(service -> () -> {
                    MockDiscordUser user = service.getUserProvider().get(user0.getId());
                    assertEquals(0, user.getStatsMessagesSent(), service.getDataManager().getServerId());
                }));

        //Validate update handlers are called on FPVs
        assertAll("update handlers called",
                discordServices.stream().map(service -> () -> {
                    MockDiscordUser user = service.getUserProvider().get(user0.getId());
                    MockDiscordUserStats stats = service.getUserStatsProvider().get(stats1.getId());
                    assertEquals(UpdatedValue.of(userName, null), stats.getLastUserNamesUpdate());
                    assertEquals(UpdatedValue.of(initialMessagesSent, null), user.getLastMessagesSentUpdate());
                }));

        //Ensure PVs are not updated when the FPV is unlinked
        assertAll("stats messages sent set",
                discordServices.stream().map(service -> () -> {
                    MockDiscordUserStats stats = service.getUserStatsProvider().get(stats1.getId());
                    assertEquals(initialMessagesSent, stats.getMessagesSent(), service.getDataManager().getServerId());
                }));

        assertAll("stats favorite number set",
                discordServices.stream().map(service -> () -> {
                    MockDiscordUserStats stats = service.getUserStatsProvider().get(stats1.getId());
                    assertEquals(favoriteNumber, stats.getFavoriteNumber(), service.getDataManager().getServerId());
                }));

        assertAll("user name set",
                discordServices.stream().map(service -> () -> {
                    MockDiscordUser user = service.getUserProvider().get(user0.getId());
                    assertEquals(userName, user.getName(), service.getDataManager().getServerId());
                }));

        assertAll("user favorite color set",
                discordServices.stream().map(service -> () -> {
                    MockDiscordUser user = service.getUserProvider().get(user0.getId());
                    assertEquals(favoriteColor, user.getFavoriteColor(), service.getDataManager().getServerId());
                }));
    }

    @RetryingTest(maxAttempts = 5, suspendForMs = 100)
    @DisplayName("Change a foreign persistent value's foreign object id")
    void changeFPVObject() {
        MockDiscordService service0 = discordServices.getFirst();
        MockDiscordService service1 = discordServices.get(1);

        //Create the user object on service0
        MockDiscordUser user0 = service0.getUserProvider().createUser("TestUser");

        //Create the stats object on service1
        MockDiscordUserStats stats1 = service1.getUserStatsProvider().createStats();

        String userName = user0.getName();
        int initialMessagesSent = 10;
        int favoriteNumber = 22;
        String favoriteColor = "blue";

        stats1.setMessagesSent(initialMessagesSent);
        stats1.setFavoriteNumber(favoriteNumber);
        user0.setFavoriteColor(favoriteColor);

        waitForDataPropagation();

        user0.setStatsId(stats1.getId());

        waitForDataPropagation();

        assertAll("foreignId set",
                discordServices.stream().map(service -> () -> {
                    MockDiscordUser user = service.getUserProvider().get(user0.getId());
                    MockDiscordUserStats stats = service.getUserStatsProvider().get(stats1.getId());
                    assertEquals(stats1.getId(), user.getStatsId(), service.getDataManager().getServerId());
                    assertEquals(user0.getId(), stats.getUserId(), service.getDataManager().getServerId());
                    assertEquals(stats1.getId(), user.getStatsFavoriteNumberFPV().getForeignObjectId(), service.getDataManager().getServerId());
                    assertEquals(user0.getId(), stats.getUserFavoriteColorFPV().getForeignObjectId(), service.getDataManager().getServerId());
                }));

        assertAll("stats name set",
                discordServices.stream().map(service -> () -> {
                    MockDiscordUserStats stats = service.getUserStatsProvider().get(stats1.getId());
                    assertEquals(userName, stats.getUserName(), service.getDataManager().getServerId());
                }));

        assertAll("user messages sent set",
                discordServices.stream().map(service -> () -> {
                    MockDiscordUser user = service.getUserProvider().get(user0.getId());
                    assertEquals(initialMessagesSent, user.getStatsMessagesSent(), service.getDataManager().getServerId());
                }));

        assertAll("stats favorite color set",
                discordServices.stream().map(service -> () -> {
                    MockDiscordUserStats stats = service.getUserStatsProvider().get(stats1.getId());
                    assertEquals(favoriteColor, stats.getUserFavoriteColor());
                }));

        /* Despite this being a different FPV entirely, it uses the same linking table,
         * so once one link is established, any other FPV with the same linking table will be updated
         */
        assertAll("user favorite number set",
                discordServices.stream().map(service -> () -> {
                    MockDiscordUser user = service.getUserProvider().get(user0.getId());
                    assertEquals(favoriteNumber, user.getStatsFavoriteNumber());
                }));

        MockDiscordUserStats newStats = service1.getUserStatsProvider().createStats();

        waitForDataPropagation();

        int newMessagesSent = 100;
        int newFavoriteNumber = 33;

        newStats.setMessagesSent(newMessagesSent);
        newStats.setFavoriteNumber(newFavoriteNumber);

        user0.setStatsId(newStats.getId());

        waitForDataPropagation();

        assertAll("foreignId set",
                discordServices.stream().map(service -> () -> {
                    MockDiscordUser user = service.getUserProvider().get(user0.getId());
                    MockDiscordUserStats stats = service.getUserStatsProvider().get(newStats.getId());
                    assertEquals(newStats.getId(), user.getStatsId(), service.getDataManager().getServerId());
                    assertEquals(user0.getId(), stats.getUserId(), service.getDataManager().getServerId());
                    assertEquals(newStats.getId(), user.getStatsFavoriteNumberFPV().getForeignObjectId(), service.getDataManager().getServerId());
                    assertEquals(user0.getId(), stats.getUserFavoriteColorFPV().getForeignObjectId(), service.getDataManager().getServerId());
                }));

        assertAll("stats name set",
                discordServices.stream().map(service -> () -> {
                    MockDiscordUserStats stats = service.getUserStatsProvider().get(newStats.getId());
                    assertEquals(userName, stats.getUserName(), service.getDataManager().getServerId());
                }));

        assertAll("user messages sent set",
                discordServices.stream().map(service -> () -> {
                    MockDiscordUser user = service.getUserProvider().get(user0.getId());
                    assertEquals(newMessagesSent, user.getStatsMessagesSent(), service.getDataManager().getServerId());
                }));

        assertAll("stats favorite color set",
                discordServices.stream().map(service -> () -> {
                    MockDiscordUserStats stats = service.getUserStatsProvider().get(newStats.getId());
                    assertEquals(favoriteColor, stats.getUserFavoriteColor());
                }));

        assertAll("user favorite number set",
                discordServices.stream().map(service -> () -> {
                    MockDiscordUser user = service.getUserProvider().get(user0.getId());
                    assertEquals(newFavoriteNumber, user.getStatsFavoriteNumber());
                }));

        //Validate the old stats object is unlinked
        assertAll("old stats name not set",
                discordServices.stream().map(service -> () -> {
                    MockDiscordUserStats stats = service.getUserStatsProvider().get(stats1.getId());
                    assertNull(stats.getUserName(), service.getDataManager().getServerId());
                }));

        assertAll("old stats favorite color not set",
                discordServices.stream().map(service -> () -> {
                    MockDiscordUserStats stats = service.getUserStatsProvider().get(stats1.getId());
                    assertNull(stats.getUserFavoriteColor(), service.getDataManager().getServerId());
                }));

        //Validate the old stats update handlers are called
        assertAll("old stats update handlers called",
                discordServices.stream().map(service -> () -> {
                    MockDiscordUserStats stats = service.getUserStatsProvider().get(stats1.getId());
                    assertEquals(UpdatedValue.of(userName, null), stats.getLastUserNamesUpdate(), service.getDataManager().getServerId());
                }));

        //Validate the new stats update handlers are called
        assertAll("new stats update handlers called",
                discordServices.stream().map(service -> () -> {
                    MockDiscordUserStats stats = service.getUserStatsProvider().get(newStats.getId());
                    assertEquals(UpdatedValue.of(null, userName), stats.getLastUserNamesUpdate());
                }));

        //Validate the user update handlers are called
        assertAll("user update handlers called",
                discordServices.stream().map(service -> () -> {
                    MockDiscordUser user = service.getUserProvider().get(user0.getId());
                    assertEquals(UpdatedValue.of(initialMessagesSent, newMessagesSent), user.getLastMessagesSentUpdate());
                }));
    }

    @RetryingTest(maxAttempts = 5, suspendForMs = 100)
    @DisplayName("Insert data into the linking table, load the data, and ensure FPV value's are present")
    void loadFPV() throws SQLException {
        MockDiscordService service0 = discordServices.getFirst();
        MockDiscordUser user0 = service0.getUserProvider().get(userIds.getFirst());
        MockDiscordUserStats stats0 = service0.getUserStatsProvider().get(userIds.getFirst());


        assertEquals(0, user0.getStatsMessagesSent());
        assertNull(stats0.getUserName());


        for (UUID userId : userIds) {
            connection.prepareStatement("INSERT INTO discord.user_stats (user_id, stats_id) VALUES ('" + userId + "', '" + userId + "')").execute();
        }

        discordServices.clear();

        UUID sessionId = UUID.randomUUID();

        for (int i = 0; i < NUM_INSTANCES; i++) {
            discordServices.add(new MockDiscordService(sessionId + "-discord-service-" + i, redis.getHost(), redis.getBindPort(), hikariConfig));
        }

        service0 = discordServices.getFirst();
        user0 = service0.getUserProvider().get(userIds.getFirst());
        stats0 = service0.getUserStatsProvider().get(userIds.getFirst());

        assertEquals(user0.getId(), stats0.getUserId());
        assertEquals(stats0.getId(), user0.getStatsId());

        assertEquals(stats0.getMessagesSent(), user0.getStatsMessagesSent());
        assertEquals(user0.getName(), stats0.getUserName());
    }

    @RetryingTest(maxAttempts = 5, suspendForMs = 100)
    @DisplayName("Insert data into the linking table, load the data, and ensure all PV & FPV update handler's are called")
    void testUpdateHandlers() {
        MockDiscordService service0 = discordServices.getFirst();
        MockDiscordUser user0 = service0.getUserProvider().get(userIds.getFirst());
        MockDiscordUserStats stats0 = service0.getUserStatsProvider().get(userIds.getFirst());

        assertAll("no updates called", discordServices.stream().map(service -> () -> {
            MockDiscordUser user = service.getUserProvider().get(userIds.getFirst());
            MockDiscordUserStats stats = service.getUserStatsProvider().get(userIds.getFirst());
            assertNull(user.getLastNameUpdate());
            assertNull(user.getLastMessagesSentUpdate());

            assertNull(stats.getLastMessagesSentUpdate());
            assertNull(stats.getLastUserNamesUpdate());
        }));

        String initialUserName = user0.getName();
        String initialStatsName = stats0.getUserName();
        int initialUserMessagesSent = user0.getStatsMessagesSent();

        user0.setStatsId(user0.getId());

        waitForDataPropagation();

        assertAll("update handlers called", discordServices.stream().map(service -> () -> {
            MockDiscordUser user = service.getUserProvider().get(userIds.getFirst());
            MockDiscordUserStats stats = service.getUserStatsProvider().get(userIds.getFirst());
            assertNull(user.getLastNameUpdate());
            assertEquals(UpdatedValue.of(initialStatsName, initialUserName), stats.getLastUserNamesUpdate());
            assertNull(stats.getLastMessagesSentUpdate());

            assertNull(user.getLastMessagesSentUpdate().oldValue());
            assertEquals(0, user.getLastMessagesSentUpdate().newValue());
        }));

        String newName = "NewName";
        user0.setName(newName);

        waitForDataPropagation();

        assertAll("update handlers called", discordServices.stream().map(service -> () -> {
            MockDiscordUser user = service.getUserProvider().get(userIds.getFirst());
            MockDiscordUserStats stats = service.getUserStatsProvider().get(userIds.getFirst());
            assertEquals(UpdatedValue.of(initialUserName, newName), user.getLastNameUpdate());
            assertEquals(UpdatedValue.of(initialUserName, newName), stats.getLastUserNamesUpdate());
        }));

        int newMessagesSent = 100;
        user0.setStatsMessagesSent(newMessagesSent);

        waitForDataPropagation();

        assertAll("update handlers called", discordServices.stream().map(service -> () -> {
            MockDiscordUser user = service.getUserProvider().get(userIds.getFirst());
            MockDiscordUserStats stats = service.getUserStatsProvider().get(userIds.getFirst());
            assertEquals(UpdatedValue.of(initialUserMessagesSent, newMessagesSent), user.getLastMessagesSentUpdate());
            assertEquals(UpdatedValue.of(initialUserMessagesSent, newMessagesSent), stats.getLastMessagesSentUpdate());
        }));
    }


    //todo: test deletions
    //todo: add more tests across different types of instances
    //todo: test to ensure the fpvs are in the lookup table after linked
    //todo: test when multiple fpvs, using different linking tables, are on an object
}