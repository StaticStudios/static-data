package net.staticstudios.data.test;

import net.staticstudios.data.mocks.netflix.MockNetflixService;
import net.staticstudios.data.mocks.netflix.MockNetflixUser;
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

public class DatabaseSupportedTypeTest extends DataTest {
    static int NUM_INSTANCES = 3;

    List<MockNetflixService> netflixServices = new ArrayList<>();

    @AfterAll
    static void teardown() throws IOException {
        postgres.stop();
        redis.stop();
    }

    @BeforeEach
    void setup() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("CREATE SCHEMA IF NOT EXISTS netflix");

            statement.execute("""
                    CREATE TABLE netflix.users (
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
        }

        UUID sessionId = UUID.randomUUID();

        for (int i = 0; i < NUM_INSTANCES; i++) {
            netflixServices.add(new MockNetflixService(sessionId + "-netflix-service-" + i, redis.getHost(), redis.getBindPort(), hikariConfig));
        }
    }

    @AfterEach
    void cleanup() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("DROP SCHEMA IF EXISTS netflix CASCADE");
        }
    }

    @RetryingTest(maxAttempts = 5, suspendForMs = 100)
    @DisplayName("Test all DatabaseSupportedTypes")
    void insertAllDatabaseSupportedTypes() {
        MockNetflixService service0 = netflixServices.getFirst();
        MockNetflixUser user0 = service0.getUserProvider().createUser();

        //Wait for the data to sync
        waitForDataPropagation();

        assertAll("data creation", netflixServices.stream().map(service -> () -> {
            MockNetflixUser user = service.getUserProvider().get(user0.getId());

            assertEquals(user0.getString(), user.getString());
            assertEquals(user0.getChar(), user.getChar());
            assertEquals(user0.getShort(), user.getShort());
            assertEquals(user0.getInt(), user.getInt());
            assertEquals(user0.getLong(), user.getLong());
            assertEquals(user0.getFloat(), user.getFloat());
            assertEquals(user0.getDouble(), user.getDouble());
            assertEquals(user0.getBoolean(), user.getBoolean());
            assertEquals(user0.getUuid(), user.getUuid());
            assertEquals(user0.getTimestamp().toInstant().toEpochMilli(), user.getTimestamp().toInstant().toEpochMilli());
            assertArrayEquals(user0.getByteArray(), user.getByteArray());
            assertArrayEquals(user0.getUuidArray(), user.getUuidArray());
        }));

        //Update each value
        user0.setString("test2");
        user0.setChar('d');
        user0.setShort((short) 2);
        user0.setInt(2);
        user0.setLong(2);
        user0.setFloat(2.0f);
        user0.setDouble(2.0);
        user0.setBoolean(false);
        user0.setUuid(UUID.randomUUID());
        user0.setTimestamp(Timestamp.from(Instant.now()));
        user0.setByteArray(new byte[]{0x02, 0x03});


        //Wait for the data to sync
        waitForDataPropagation();

        assertAll("data update", netflixServices.stream().map(service -> () -> {
            MockNetflixUser user = service.getUserProvider().get(user0.getId());

            assertEquals(user0.getString(), user.getString());
            assertEquals(user0.getChar(), user.getChar());
            assertEquals(user0.getShort(), user.getShort());
            assertEquals(user0.getInt(), user.getInt());
            assertEquals(user0.getLong(), user.getLong());
            assertEquals(user0.getFloat(), user.getFloat());
            assertEquals(user0.getDouble(), user.getDouble());
            assertEquals(user0.getBoolean(), user.getBoolean());
            assertEquals(user0.getUuid(), user.getUuid());
            assertEquals(user0.getTimestamp().toInstant().toEpochMilli(), user.getTimestamp().toInstant().toEpochMilli());
            assertArrayEquals(user0.getByteArray(), user.getByteArray());
            assertArrayEquals(user0.getUuidArray(), user.getUuidArray());
        }));
    }

}