package net.staticstudios.data.test;

import net.staticstudios.data.UpdatedValue;
import net.staticstudios.data.mocks.discord.MockDiscordService;
import net.staticstudios.data.mocks.discord.MockDiscordUser;
import net.staticstudios.data.mocks.discord.MockDiscordUserStats;
import net.staticstudios.data.util.DataTest;
import org.junit.jupiter.api.*;
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
                        name TEXT NOT NULL
                    )
                    """);

            statement.execute("""
                    CREATE TABLE discord.stats (
                        id uuid PRIMARY KEY,
                        messages_sent integer NOT NULL DEFAULT 0
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

        assertEquals(10, user0.getMessagesSent());
        assertEquals("TestUser", stats1.getName());
    }

    @RetryingTest(maxAttempts = 5, suspendForMs = 100)
    @DisplayName("Update a foreign persistent value and ensure it is consistent across all instances")
    void updateFPV() {
        MockDiscordService service0 = discordServices.getFirst();
        MockDiscordService service1 = discordServices.get(1);

        //Create the user object on service0
        MockDiscordUser user0 = service0.getUserProvider().createUser("TestUser");

        //Create the stats object on service1
        MockDiscordUserStats stats1 = service1.getUserStatsProvider().createStats();

        int initialMessagesSent = 10;

        //Set the initial value to 10
        stats1.setMessagesSent(initialMessagesSent);

        waitForDataPropagation();

        assertAll("stats messages set",
                discordServices.stream().map(service -> () -> {
                    MockDiscordUserStats stats = service.getUserStatsProvider().get(stats1.getId());
                    assertEquals(initialMessagesSent, stats.getMessagesSent());
                }));

        assertAll("user name not set",
                discordServices.stream().map(service -> () -> {
                    MockDiscordUserStats stats = service.getUserStatsProvider().get(stats1.getId());
                    assertNull(stats.getName());
                }));

        //Link the user to the stats
        user0.setStatsId(stats1.getId());

        waitForDataPropagation();

        //todo: add other fpvs to the objects and ensure theyre linked

        assertAll("foreignId set",
                discordServices.stream().map(service -> () -> {
                    MockDiscordUser user = service.getUserProvider().get(user0.getId());
                    MockDiscordUserStats stats = service.getUserStatsProvider().get(stats1.getId());
                    assertEquals(stats1.getId(), user.getStatsId(), service.getDataManager().getServerId());
                    assertEquals(user0.getId(), stats.getUserId(), service.getDataManager().getServerId());
                }));

        /* Ensure that the name is now set on the stats object
         * Despite this being a different FPV entirely, it uses the same linking table,
         * so once one link is established, any other FPV with the same linking table will be updated
         */
        assertAll("stats name set",
                discordServices.stream().map(service -> () -> {
                    MockDiscordUserStats stats = service.getUserStatsProvider().get(stats1.getId());
                    assertEquals(user0.getName(), stats.getName(), service.getDataManager().getServerId());
                }));

        MockDiscordUserStats stats0 = service0.getUserStatsProvider().get(stats1.getId());
        user0.incrementMessagesSent(); //Update the value from an FPV on service0
        assertEquals(11, stats0.getMessagesSent()); //The PV on service0 should update instantly, since it is local

        waitForDataPropagation();

        //Validate the data actually propagated
        //todo: we should then test unlinking, and ensure the data gets set to null

        assertAll("stats messages sent updated",
                discordServices.stream().map(service -> () -> {
                    MockDiscordUserStats stats = service.getUserStatsProvider().get(stats1.getId());
                    assertEquals(11, stats.getMessagesSent());
                }));
    }

    @RetryingTest(maxAttempts = 5, suspendForMs = 100)
    @DisplayName("Insert data into the linking table, load the data, and ensure FPV value's are present")
    void loadFPV() throws SQLException {
        MockDiscordService service0 = discordServices.getFirst();
        MockDiscordUser user0 = service0.getUserProvider().get(userIds.getFirst());
        MockDiscordUserStats stats0 = service0.getUserStatsProvider().get(userIds.getFirst());


        assertEquals(-1, user0.getMessagesSent());
        assertNull(stats0.getName());


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

        assertEquals(stats0.getMessagesSent(), user0.getMessagesSent());
        assertEquals(user0.getName(), stats0.getName());
    }

    @Disabled("This test is failing, since update handlers arent implemented properly yet")
    @RetryingTest(maxAttempts = 5, suspendForMs = 100)
    @DisplayName("Insert data into the linking table, load the data, and ensure all PV & FPV update handler's are called")
    void testUpdateHandlers() {
        MockDiscordService service0 = discordServices.getFirst();
        MockDiscordUser user0 = service0.getUserProvider().get(userIds.getFirst());
        MockDiscordUserStats stats0 = service0.getUserStatsProvider().get(userIds.getFirst());

        assertAll("no updates called", discordServices.stream().map(service -> () -> {
            MockDiscordUser user = service.getUserProvider().get(userIds.getFirst());
            MockDiscordUserStats stats = service.getUserStatsProvider().get(userIds.getFirst());
            assertNull(user.getLastNamesUpdate());
            assertNull(user.getLastMessagesSentUpdate());

            assertNull(stats.getLastMessagesSentUpdate());
            assertNull(stats.getLastNamesUpdate());
        }));

        String initialUserName = user0.getName();
        String initialStatsName = stats0.getName();
        int initialUserMessagesSent = user0.getMessagesSent();
        int initialStatsMessagesSent = stats0.getMessagesSent();

        user0.setStatsId(user0.getId());

        waitForDataPropagation();

        assertAll("update handlers called", discordServices.stream().map(service -> () -> {
            MockDiscordUser user = service.getUserProvider().get(userIds.getFirst());
            MockDiscordUserStats stats = service.getUserStatsProvider().get(userIds.getFirst());
            assertNull(user.getLastNamesUpdate());
            assertEquals(UpdatedValue.of(initialStatsName, initialUserName), stats.getLastNamesUpdate());
            assertNull(stats.getLastMessagesSentUpdate());
            assertEquals(UpdatedValue.of(initialStatsMessagesSent, initialUserMessagesSent), user.getLastMessagesSentUpdate());
        }));

        String newName = "NewName";
        user0.setName(newName);

        waitForDataPropagation();

        assertAll("update handlers called", discordServices.stream().map(service -> () -> {
            MockDiscordUser user = service.getUserProvider().get(userIds.getFirst());
            MockDiscordUserStats stats = service.getUserStatsProvider().get(userIds.getFirst());
            assertEquals(UpdatedValue.of(initialUserName, newName), user.getLastNamesUpdate());
            assertEquals(UpdatedValue.of(initialUserName, newName), stats.getLastNamesUpdate());
        }));

        int newMessagesSent = 100;
        user0.setMessagesSent(newMessagesSent);

        waitForDataPropagation();

        assertAll("update handlers called", discordServices.stream().map(service -> () -> {
            MockDiscordUser user = service.getUserProvider().get(userIds.getFirst());
            MockDiscordUserStats stats = service.getUserStatsProvider().get(userIds.getFirst());
            assertEquals(UpdatedValue.of(initialUserMessagesSent, newMessagesSent), user.getLastMessagesSentUpdate());
            assertEquals(UpdatedValue.of(initialUserMessagesSent, newMessagesSent), stats.getLastMessagesSentUpdate());
        }));
    }

    //todo: test the update handler on local fpvs, local pvs, remote fpvs, and remote pvs

    //todo: test deletions
    //todo: add more tests across different types of instances
    //todo: test to ensure the fpvs are in the lookup table after linked
}