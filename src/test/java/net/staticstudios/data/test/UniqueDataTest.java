package net.staticstudios.data.test;

import net.staticstudios.data.DeletionType;
import net.staticstudios.data.mocks.MockLocation;
import net.staticstudios.data.mocks.MockOnlineStatus;
import net.staticstudios.data.mocks.MockTestDataObject;
import net.staticstudios.data.mocks.MockThreadProvider;
import net.staticstudios.data.mocks.game.prison.MockPrisonGame;
import net.staticstudios.data.mocks.game.prison.MockPrisonPlayer;
import net.staticstudios.data.mocks.game.skyblock.MockSkyblockGame;
import net.staticstudios.data.mocks.game.skyblock.MockSkyblockPlayer;
import net.staticstudios.data.mocks.service.MockService;
import net.staticstudios.data.util.DataTest;
import net.staticstudios.utils.ThreadUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junitpioneer.jupiter.RetryingTest;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;


//todo: split this up into multiple test classes
public class UniqueDataTest extends DataTest {
    static int NUM_INSTANCES = 3;
    static int NUM_USERS = 10;
    static int NUM_HOME_LOCATIONS = 3;

    List<MockSkyblockGame> skyblockGameInstances = new ArrayList<>();
    List<UUID> userIds = new ArrayList<>();


    @AfterAll
    static void teardown() throws IOException {
        postgres.stop();
        redis.stop();
    }

    @BeforeEach
    void setup() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("CREATE SCHEMA IF NOT EXISTS skyblock");
            statement.execute("CREATE SCHEMA IF NOT EXISTS prison");

            statement.execute("""
                    CREATE TABLE users (
                        id uuid PRIMARY KEY,
                        name varchar(255) NOT NULL
                    )
                    """);

            statement.execute("""
                    CREATE TABLE players (
                        id uuid PRIMARY KEY,
                        money bigint NOT NULL DEFAULT 0,
                        favorite_block varchar(255) NOT NULL DEFAULT '0,0,0'
                    )
                    """);

            statement.execute("""
                    CREATE TABLE skyblock.players (
                        id uuid PRIMARY KEY,
                        sky_coins bigint NOT NULL DEFAULT 100
                    )
                    """);

            statement.execute("""
                    CREATE TABLE prison.players (
                        id uuid PRIMARY KEY,
                        prison_coins bigint NOT NULL DEFAULT 100
                    )
                    """);

            statement.execute("""
                    CREATE TABLE home_locations (
                        id uuid NOT NULL,
                        player_id uuid NOT NULL,
                        x int NOT NULL,
                        y int NOT NULL,
                        z int NOT NULL,
                        PRIMARY KEY (id, player_id)
                    )
                    """);

            statement.execute("""
                    CREATE TABLE test_data_types (
                        id uuid PRIMARY KEY,
                        test_string varchar(255) NOT NULL DEFAULT 'test',
                        test_char char NOT NULL DEFAULT 'c',
                        test_short smallint NOT NULL DEFAULT 1,
                        test_int int NOT NULL DEFAULT 1,
                        test_long bigint NOT NULL DEFAULT 1,
                        test_float real NOT NULL DEFAULT 1.0,
                        test_double double precision NOT NULL DEFAULT 1.0,
                        test_boolean boolean NOT NULL DEFAULT true,
                        test_uuid uuid NOT NULL DEFAULT gen_random_uuid(),
                        test_timestamp timestamp NOT NULL DEFAULT '1970-01-01 00:00:00',
                        test_byte_array bytea NOT NULL DEFAULT E'\\\\x010203',
                        test_uuid_array uuid[] NOT NULL DEFAULT ARRAY[gen_random_uuid()]
                    )
                    """);

            //Insert some data
            for (int i = 0; i < NUM_USERS; i++) {
                UUID userId = UUID.randomUUID();
                userIds.add(userId);
                statement.execute("INSERT INTO users (id, name) VALUES ('%s', 'user_%s')".formatted(userId, i));
                statement.execute("INSERT INTO players (id) VALUES ('%s')".formatted(userId));
                statement.execute("INSERT INTO skyblock.players (id) VALUES ('%s')".formatted(userId));

                for (int j = 0; j < NUM_HOME_LOCATIONS; j++) {
                    statement.execute("INSERT INTO home_locations (id, player_id, x, y, z) VALUES ('%s', '%s', %s, %s, %s)".formatted(UUID.randomUUID(), userId, j, j, j));
                }
            }
        }

        UUID sessionId = UUID.randomUUID();

        for (int i = 0; i < NUM_INSTANCES; i++) {
            skyblockGameInstances.add(new MockSkyblockGame(sessionId + "-game-" + i, redis.getHost(), redis.getBindPort(), hikariConfig));
        }
    }

    @AfterEach
    void cleanup() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE users");
            statement.execute("DROP TABLE players");
            statement.execute("DROP TABLE skyblock.players");
            statement.execute("DROP TABLE prison.players");
            statement.execute("DROP TABLE home_locations");
            statement.execute("DROP TABLE test_data_types");
        }
    }

    @DisplayName("Ensure every skyblock game has the same players")
    @RetryingTest(maxAttempts = 5, suspendForMs = 100)
    void verifyPlayers() {
        MockSkyblockGame skyblockGame = skyblockGameInstances.getFirst();

        assertAll("Players are not consistent across skyblock instances", () -> {
            for (int i = 1; i < NUM_INSTANCES; i++) {
                MockSkyblockGame g = skyblockGameInstances.get(i);
                for (MockSkyblockPlayer p : skyblockGame.getPlayerProvider().getAll()) {
                    assertNotNull(g.getPlayerProvider().get(p.getId()));
                }
            }
        });
    }


    @RetryingTest(maxAttempts = 5, suspendForMs = 100)
    @DisplayName("Update a skyblock player's money")
    void updateMoney() throws InterruptedException {
        MockSkyblockGame skyblockGame = skyblockGameInstances.getFirst();
        UUID playerId = userIds.getFirst();

        MockSkyblockPlayer player = skyblockGame.getPlayerProvider().get(playerId);
        player.setMoney(5);

        //Wait for the data to sync
        waitForDataPropagation();

        assertAll("Money is not consistent across skyblock instances", () -> {
                    for (int i = 1; i < NUM_INSTANCES; i++) {
                        MockSkyblockPlayer p = skyblockGameInstances.get(i).getPlayerProvider().get(playerId);
                        assertEquals(5, p.getMoney());
                    }
                }
        );
    }


    @RetryingTest(maxAttempts = 5, suspendForMs = 100)
    @DisplayName("Update a skyblock player's (user's) name")
    void updateName() throws InterruptedException {
        //The name exists on the user, not the player
        MockSkyblockGame skyblockGame = skyblockGameInstances.getFirst();
        UUID playerId = userIds.getFirst();

        MockSkyblockPlayer player = skyblockGame.getPlayerProvider().get(playerId);
        player.setName("Dan");

        //Wait for the data to sync
        waitForDataPropagation();

        assertAll("Name is not consistent across skyblock instances", () -> {
                    for (int i = 1; i < NUM_INSTANCES; i++) {
                        MockSkyblockPlayer p = skyblockGameInstances.get(i).getPlayerProvider().get(playerId);
                        assertEquals("Dan", p.getName());
                    }
                }
        );
    }


    @RetryingTest(maxAttempts = 5, suspendForMs = 100)
    @DisplayName("Create a skyblock player with default values")
    void createPlayerWithDefaults() throws InterruptedException {
        MockSkyblockGame skyblockGame = skyblockGameInstances.getFirst();
        String name = "John";

        MockSkyblockPlayer player = new MockSkyblockPlayer(skyblockGame.getDataManager(), name);
        UUID playerId = player.getId();
        skyblockGame.getPlayerProvider().set(player);

        //Wait for the data to sync
        waitForDataPropagation();

        assertAll("Name is not consistent across skyblock instances", () -> {
            for (int i = 1; i < NUM_INSTANCES; i++) {
                MockSkyblockPlayer p = skyblockGameInstances.get(i).getPlayerProvider().get(playerId);
                assertEquals(name, p.getName());
            }
        });
    }


    @RetryingTest(maxAttempts = 5, suspendForMs = 100)
    @DisplayName("Create a skyblock player with 2 distinct initial home locations, then add another, and ensure that the player has 3 home locations")
    void createPlayerWithDistinctHomeLocations() throws InterruptedException {
        MockSkyblockGame skyblockGame = skyblockGameInstances.getFirst();

        MockSkyblockPlayer player = new MockSkyblockPlayer(skyblockGame.getDataManager(), "John",
                new MockLocation(skyblockGame.getDataManager(), 0, 0, 0),
                new MockLocation(skyblockGame.getDataManager(), 1, 1, 1)
        );
        UUID playerId = player.getId();
        skyblockGame.getPlayerProvider().set(player);

        //Wait for the data to sync
        waitForDataPropagation();

        assertAll("Home locations are not consistent across skyblock instances", () -> {
            for (int i = 1; i < NUM_INSTANCES; i++) {
                MockSkyblockPlayer p = skyblockGameInstances.get(i).getPlayerProvider().get(playerId);
                assertEquals(2, p.getHomeLocations().size());
            }
        });

        player.addHomeLocation(skyblockGame.getDataManager(), 2, 2, 2);

        assertEquals(3, player.getHomeLocations().size(), "Player does not have 3 home locations");

        //Wait for the data to sync
        waitForDataPropagation();

        assertAll("Home locations are not consistent across skyblock instances", () -> {
            for (int i = 1; i < NUM_INSTANCES; i++) {
                MockSkyblockPlayer p = skyblockGameInstances.get(i).getPlayerProvider().get(playerId);
                assertEquals(3, p.getHomeLocations().size());
            }
        });
    }


    @RetryingTest(maxAttempts = 5, suspendForMs = 100)
    @DisplayName("Create a player with 2 of the same initial home locations, remove one, and ensure that the player has only one home location")
    void createPlayerWithSameHomeLocations() throws InterruptedException {
        MockSkyblockGame skyblockGame = skyblockGameInstances.getFirst();

        MockSkyblockPlayer player = new MockSkyblockPlayer(skyblockGame.getDataManager(), "John",
                new MockLocation(skyblockGame.getDataManager(), 0, 0, 0),
                new MockLocation(skyblockGame.getDataManager(), 0, 0, 0)
        );
        UUID playerId = player.getId();
        skyblockGame.getPlayerProvider().set(player);

        //Wait for the data to sync
        waitForDataPropagation();

        assertAll("Home locations are not consistent across skyblock instances", () -> {
            for (int i = 1; i < NUM_INSTANCES; i++) {
                MockSkyblockPlayer p = skyblockGameInstances.get(i).getPlayerProvider().get(playerId);
                assertEquals(2, p.getHomeLocations().size());
            }
        });

        player.removeHomeLocation(skyblockGame.getDataManager(), 0, 0, 0);

        assertEquals(1, player.getHomeLocations().size(), "Player does not have only one home location");

        //Wait for the data to sync
        waitForDataPropagation();

        assertAll("Home locations are not consistent across skyblock instances", () -> {
            for (int i = 1; i < NUM_INSTANCES; i++) {
                MockSkyblockPlayer p = skyblockGameInstances.get(i).getPlayerProvider().get(playerId);
                assertEquals(1, p.getHomeLocations().size());
            }
        });
    }


    @RetryingTest(maxAttempts = 5, suspendForMs = 100)
    @DisplayName("Update a player's home location")
    void updateHomeLocation() throws InterruptedException {
        MockSkyblockGame skyblockGame = skyblockGameInstances.getFirst();

        MockSkyblockPlayer player = skyblockGame.getPlayerProvider().get(userIds.getFirst());
        MockLocation location = player.getHomeLocations().stream().findFirst().orElse(null);

        assertNotNull(location, "Player does not have a home location");

        location.setX(5);
        location.setY(5);
        location.setZ(5);

        location.update(player.getHomeLocations());

        //Wait for the data to sync
        waitForDataPropagation();

        assertAll("Home locations are not consistent across skyblock instances", () -> {
            for (int i = 1; i < NUM_INSTANCES; i++) {
                MockSkyblockPlayer p = skyblockGameInstances.get(i).getPlayerProvider().get(userIds.getFirst());
                MockLocation l = p.getHomeLocations().stream().filter(loc -> loc.getX() == 5 && loc.getY() == 5 && loc.getZ() == 5).findFirst().orElse(null);
                assertNotNull(l, "Home location does not exist on " + skyblockGameInstances.get(i).getDataManager().getServerId());
            }
        });

        //Restart the game to ensure its read from the db properly
        ThreadUtils.shutdown();
        ThreadUtils.setProvider(new MockThreadProvider());

        MockSkyblockGame newSkyblockGame = new MockSkyblockGame("new-skyblock", redis.getHost(), redis.getBindPort(), hikariConfig);
        MockSkyblockPlayer newPlayer = newSkyblockGame.getPlayerProvider().get(userIds.getFirst());

        MockLocation newLocation = newPlayer.getHomeLocations().stream().filter(loc -> loc.getX() == 5 && loc.getY() == 5 && loc.getZ() == 5).findFirst().orElse(null);
        assertNotNull(newLocation, "Home location does not exist on new skyblock instance");

    }


    @RetryingTest(maxAttempts = 5, suspendForMs = 100)
    @DisplayName("Create a player on skyblock, and verify the player does not exist on prison")
    void createSkyblockPlayerAndVerifyPrisonPlayerNotExist() throws InterruptedException {
        MockSkyblockGame skyblockGame = skyblockGameInstances.getFirst();
        MockPrisonGame prisonGame = new MockPrisonGame("prison", redis.getHost(), redis.getBindPort(), hikariConfig);

        MockSkyblockPlayer player = new MockSkyblockPlayer(skyblockGame.getDataManager(), "John");
        UUID playerId = player.getId();
        skyblockGame.getPlayerProvider().set(player);

        //Wait for the data to sync
        waitForDataPropagation();

        assertNull(prisonGame.getPlayerProvider().get(playerId), "Player exists on prison");
    }


    @RetryingTest(maxAttempts = 5, suspendForMs = 100)
    @DisplayName("Create a player on skyblock, give it some money, create a player on prison with the same id, and verify the money is equal to skyblock")
    void createSkyblockPlayerAndVerifyPrisonPlayerMoney() throws InterruptedException {
        MockSkyblockGame skyblockGame = skyblockGameInstances.getFirst();
        MockPrisonGame prisonGame = new MockPrisonGame("prison", redis.getHost(), redis.getBindPort(), hikariConfig);

        UUID playerId = UUID.randomUUID();

        MockSkyblockPlayer skyblockPlayer = new MockSkyblockPlayer(skyblockGame.getDataManager(), playerId, "John");
        skyblockGame.getPlayerProvider().set(skyblockPlayer);

        skyblockPlayer.setMoney(5);

        //Wait for the data to sync
        waitForDataPropagation();

        MockPrisonPlayer prisonPlayer = new MockPrisonPlayer(prisonGame.getDataManager(), playerId, "Dan");
        prisonGame.getPlayerProvider().set(prisonPlayer);

        assertEquals(5, prisonPlayer.getMoney(), "Money is not consistent across skyblock and prison");

        //Since the name "John" already existed on the user before we created the prison player, the name should be "John" should still be on the user.
        //Information already in the db should not be overridden when creating a new object that "extends" the user.
        assertNotEquals("Dan", prisonPlayer.getName(), "Name should not be overridden when creating a new object that extends the user");
    }


    @RetryingTest(maxAttempts = 5, suspendForMs = 100)
    @DisplayName("Test all DatabaseSupportedTypes")
    void insertAllDatabaseSupportedTypes() throws InterruptedException {
        MockService service = new MockService("service", redis.getHost(), redis.getBindPort(), hikariConfig);
        MockService service2 = new MockService("service-2", redis.getHost(), redis.getBindPort(), hikariConfig);

        MockTestDataObject testDataObject = service.createTestDataObject();

        //Wait for the data to sync
        waitForDataPropagation();

        MockTestDataObject testDataObject2 = service2.getDataObjectProvider().get(testDataObject.getId());

        assertAll("Initial values are not consistent", () -> {
            assertEquals(testDataObject.getString(), testDataObject2.getString());
            assertEquals(testDataObject.getChar(), testDataObject2.getChar());
            assertEquals(testDataObject.getShort(), testDataObject2.getShort());
            assertEquals(testDataObject.getInt(), testDataObject2.getInt());
            assertEquals(testDataObject.getLong(), testDataObject2.getLong());
            assertEquals(testDataObject.getFloat(), testDataObject2.getFloat());
            assertEquals(testDataObject.getDouble(), testDataObject2.getDouble());
            assertEquals(testDataObject.getBoolean(), testDataObject2.getBoolean());
            assertEquals(testDataObject.getUuid(), testDataObject2.getUuid());
            assertEquals(testDataObject.getTimestamp().toInstant().toEpochMilli(), testDataObject2.getTimestamp().toInstant().toEpochMilli());
            assertArrayEquals(testDataObject.getByteArray(), testDataObject2.getByteArray());
            assertArrayEquals(testDataObject.getUuidArray(), testDataObject2.getUuidArray());
        });

        //Update each value
        testDataObject.setString("test2");
        testDataObject.setChar('d');
        testDataObject.setShort((short) 2);
        testDataObject.setInt(2);
        testDataObject.setLong(2);
        testDataObject.setFloat(2.0f);
        testDataObject.setDouble(2.0);
        testDataObject.setBoolean(false);
        testDataObject.setUuid(UUID.randomUUID());
        testDataObject.setTimestamp(Timestamp.from(Instant.now()));
        testDataObject.setByteArray(new byte[]{0x02, 0x03});


        //Wait for the data to sync
        waitForDataPropagation();

        assertAll("Updated values are not consistent", () -> {
            assertEquals(testDataObject.getString(), testDataObject2.getString());
            assertEquals(testDataObject.getChar(), testDataObject2.getChar());
            assertEquals(testDataObject.getShort(), testDataObject2.getShort());
            assertEquals(testDataObject.getInt(), testDataObject2.getInt());
            assertEquals(testDataObject.getLong(), testDataObject2.getLong());
            assertEquals(testDataObject.getFloat(), testDataObject2.getFloat());
            assertEquals(testDataObject.getDouble(), testDataObject2.getDouble());
            assertEquals(testDataObject.getBoolean(), testDataObject2.getBoolean());
            assertEquals(testDataObject.getUuid(), testDataObject2.getUuid());
            assertEquals(testDataObject.getTimestamp().toInstant().toEpochMilli(), testDataObject2.getTimestamp().toInstant().toEpochMilli());
            assertArrayEquals(testDataObject.getByteArray(), testDataObject2.getByteArray());
            assertArrayEquals(testDataObject.getUuidArray(), testDataObject2.getUuidArray());
        });

        MockService service3 = new MockService("service-3", redis.getHost(), redis.getBindPort(), hikariConfig);
        MockTestDataObject testDataObject3 = service3.getDataObjectProvider().get(testDataObject.getId());

        assertAll("Read values are not consistent", () -> {
            assertEquals(testDataObject.getString(), testDataObject3.getString());
            assertEquals(testDataObject.getChar(), testDataObject3.getChar());
            assertEquals(testDataObject.getShort(), testDataObject3.getShort());
            assertEquals(testDataObject.getInt(), testDataObject3.getInt());
            assertEquals(testDataObject.getLong(), testDataObject3.getLong());
            assertEquals(testDataObject.getFloat(), testDataObject3.getFloat());
            assertEquals(testDataObject.getDouble(), testDataObject3.getDouble());
            assertEquals(testDataObject.getBoolean(), testDataObject3.getBoolean());
            assertEquals(testDataObject.getUuid(), testDataObject3.getUuid());
            assertEquals(testDataObject.getTimestamp().toInstant().toEpochMilli(), testDataObject3.getTimestamp().toInstant().toEpochMilli());
            assertArrayEquals(testDataObject.getByteArray(), testDataObject3.getByteArray());
            assertArrayEquals(testDataObject.getUuidArray(), testDataObject3.getUuidArray());
        });
    }


    @RetryingTest(maxAttempts = 5, suspendForMs = 100)
    @DisplayName("Set a player's online status to offline, then back to online")
    void changeOnlineStatus() throws InterruptedException {
        MockSkyblockGame skyblockGame = skyblockGameInstances.getFirst();
        UUID playerId = userIds.getFirst();


        String key = skyblockGame.getDataManager().buildCachedValueKey("online_status", "skyblock.players", playerId);
        try (Jedis jedis = skyblockGame.getDataManager().getJedis()) {
            assertFalse(jedis.exists(key), "Online status key exists");
        }

        MockSkyblockPlayer player = skyblockGame.getPlayerProvider().get(playerId);
        player.setOnlineStatus(MockOnlineStatus.ONLINE);

        //Wait for the data to sync
        waitForDataPropagation();

        assertAll("Online status is not consistent across skyblock instances", () -> {
            for (int i = 1; i < NUM_INSTANCES; i++) {
                MockSkyblockPlayer p = skyblockGameInstances.get(i).getPlayerProvider().get(playerId);
                assertEquals(MockOnlineStatus.ONLINE, p.getOnlineStatus(), skyblockGameInstances.get(i).getDataManager().getServerId() + " online status is not consistent");
            }
        });


        try (Jedis jedis = skyblockGame.getDataManager().getJedis()) {
            assertTrue(jedis.exists(key), "Online status key does not exist");
        }

        player.setOnlineStatus(MockOnlineStatus.OFFLINE);

        //Wait for the data to sync
        waitForDataPropagation();

        assertAll("Online status is not consistent across skyblock instances", () -> {
            for (int i = 1; i < NUM_INSTANCES; i++) {
                MockSkyblockPlayer p = skyblockGameInstances.get(i).getPlayerProvider().get(playerId);
                assertEquals(MockOnlineStatus.OFFLINE, p.getOnlineStatus());
            }
        });

        try (Jedis jedis = skyblockGame.getDataManager().getJedis()) {
            assertFalse(jedis.exists(key), "Online status key exists");
        }

        player.setOnlineStatus(MockOnlineStatus.ONLINE);

        //Wait for the data to sync
        waitForDataPropagation();

        MockSkyblockGame newSkyblockGame = new MockSkyblockGame("new-skyblock", redis.getHost(), redis.getBindPort(), hikariConfig);
        MockSkyblockPlayer newPlayer = newSkyblockGame.getPlayerProvider().get(playerId);

        assertEquals(MockOnlineStatus.ONLINE, newPlayer.getOnlineStatus(), "Online status is not consistent across skyblock instances");
    }


    @RetryingTest(maxAttempts = 5, suspendForMs = 100)
    @DisplayName("Delete a player")
    void deletePlayer() throws InterruptedException {
        MockSkyblockGame skyblockGame = skyblockGameInstances.getFirst();
        MockSkyblockPlayer player = skyblockGame.getPlayerProvider().get(userIds.getFirst());

        assertNotNull(player, "Player does not exist");

        skyblockGame.getDataManager().delete(player, DeletionType.ALL);

        //Wait for the data to sync
        waitForDataPropagation();

        assertAll("Player still exists", () -> {
            for (int i = 1; i < NUM_INSTANCES; i++) {
                MockSkyblockPlayer p = skyblockGameInstances.get(i).getPlayerProvider().get(userIds.getFirst());
                assertNull(p, "Player exists on " + skyblockGameInstances.get(i).getDataManager().getServerId());
            }
        });
    }
}
//todo: test the update handlers, add handlers, and remove handlers extensively