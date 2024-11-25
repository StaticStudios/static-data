package net.staticstudios.data.test;

import net.staticstudios.data.mocks.discord.MockDiscordBot;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class ForeignPersistentValuesTest extends DataTest {
    static int NUM_INSTANCES = 3;
    static int NUM_USERS = 0;

    List<MockDiscordBot> discordBots = new ArrayList<>();
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
                statement.execute("INSERT INTO discord.user_stats (user_id, stats_id) VALUES ('" + userId + "', '" + userId + "')");
            }
        }

        UUID sessionId = UUID.randomUUID();

        for (int i = 0; i < NUM_INSTANCES; i++) {
            discordBots.add(new MockDiscordBot(sessionId + "-discord-bot-" + i, redis.getHost(), redis.getBindPort(), hikariConfig));
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
    void insertFPVCheckInitialValue() throws InterruptedException {
        MockDiscordBot bot0 = discordBots.getFirst();
        MockDiscordBot bot1 = discordBots.get(1);

        //Create the user object on bot0
        MockDiscordUser user0 = bot0.getUserProvider().createUser("TestUser");
        UUID userId = UUID.randomUUID();

        //Create the stats object on bot1, and set the initial messages sent to 10
        MockDiscordUserStats stats1 = bot1.getUserStatsProvider().createStats();
        stats1.setMessagesSent(10);

        waitForDataPropagation();

        user0.setStatsId(stats1.getId());

        waitForDataPropagation();

        assertEquals(10, user0.getMessagesSent());
        assertEquals("TestUser", stats1.getName());
    }

    @RetryingTest(maxAttempts = 5, suspendForMs = 100)
    @DisplayName("Update a foreign persistent value and ensure it is consistent across all instances")
    void updateFPV() throws InterruptedException {
        MockDiscordBot bot0 = discordBots.getFirst();
        MockDiscordBot bot1 = discordBots.get(1);

        //Create the user object on bot0
        MockDiscordUser user0 = bot0.getUserProvider().createUser("TestUser");
        UUID userId = user0.getId();

        //Create the stats object on bot1
        MockDiscordUserStats stats1 = bot1.getUserStatsProvider().createStats();
        UUID statsId = stats1.getId();

        //Set the initial value to 10
        stats1.setMessagesSent(10);

        waitForDataPropagation();

        //Ensure that the name is not set, since the user and stats are not linked
        assertNull(stats1.getName());

        //Ensure the stats object was created on bot0
        MockDiscordUserStats stats0 = bot0.getUserStatsProvider().get(statsId);
        assertEquals(10, stats0.getMessagesSent());

        //Ensure the stats object was created on bot2
        MockDiscordUserStats stats2 = bot1.getUserStatsProvider().get(statsId);
        assertEquals(10, stats2.getMessagesSent());

        //Link the user to the stats
        user0.setStatsId(statsId);

        waitForDataPropagation();

        /* Ensure that the name is now set on the stats object
         * Despite this being a different FPV entirely, it uses the same linking table,
         * so once one link is established, any other FPV with the same linking table will be updated
         */
        assertEquals("TestUser", stats1.getName());

        user0.incrementMessagesSent(); //Update the value from an FPV on bot0
        assertEquals(11, stats0.getMessagesSent()); //The PV on bot0 should update instantly, since it is local

        waitForDataPropagation();

        //Validate the data actually propagated
        //todo: we should then test unlinking, and ensure the data gets set to null

        assertEquals(11, stats1.getMessagesSent());
        assertEquals(11, stats2.getMessagesSent());


    }

    //todo: test deletions
    //todo: add more tests across different types of instances
}