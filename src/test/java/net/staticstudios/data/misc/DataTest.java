package net.staticstudios.data.misc;

import com.redis.testcontainers.RedisContainer;
import net.staticstudios.data.DataManager;
import net.staticstudios.data.impl.h2.H2DataAccessor;
import net.staticstudios.data.util.DataSourceConfig;
import net.staticstudios.utils.ThreadUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.*;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

public class DataTest {
    //todo: performance test static-data using: java microbenchmarking harness
    public static int NUM_ENVIRONMENTS = 1;
    public static RedisContainer redis;
    public static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            "postgres:16.2"
    )
            .withExposedPorts(5432)
            .withPassword("password")
            .withUsername("postgres")
            .withDatabaseName("postgres");
    public static DataSourceConfig dataSourceConfig;
    private static Connection connection;
    private static Jedis jedis;
    private List<MockEnvironment> mockEnvironments;

    @BeforeAll
    static void initPostgres() throws IOException, SQLException, InterruptedException {
        postgres.start();
        redis = new RedisContainer(DockerImageName.parse("redis:6.2.6"));
        redis.start();

        redis.execInContainer("redis-cli", "config", "set", "notify-keyspace-events", "KEA");

        dataSourceConfig = new DataSourceConfig(
                postgres.getHost(),
                postgres.getFirstMappedPort(),
                postgres.getDatabaseName(),
                postgres.getUsername(),
                postgres.getPassword(),
                redis.getHost(),
                redis.getRedisPort()
        );

        connection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
        jedis = new Jedis(redis.getHost(), redis.getRedisPort());
    }

    @AfterAll
    public static void cleanup() throws IOException {
        postgres.stop();
        redis.stop();
    }

    public static Connection getConnection() {
        return connection;
    }

    public static Jedis getJedis() {
        return jedis;
    }

    @BeforeEach
    public void setupMockEnvironments() {
        mockEnvironments = new LinkedList<>();
        ThreadUtils.setProvider(new MockThreadProvider());
        for (int i = 0; i < NUM_ENVIRONMENTS; i++) {
            mockEnvironments.add(createMockEnvironment());
        }
    }

    protected MockEnvironment createMockEnvironment() {
        DataManager dataManager = new DataManager(dataSourceConfig, false);

        MockEnvironment mockEnvironment = new MockEnvironment(dataSourceConfig, dataManager);
        mockEnvironments.add(mockEnvironment);
        return mockEnvironment;
    }

    @AfterEach
    public void teardownThreadUtils() {
        ThreadUtils.shutdown();
    }

    @AfterEach
    public void wipeDatabase() throws SQLException {
        try (Statement statement = getConnection().createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'public', 'pg_toast')")
        ) {
            List<String> schemas = new LinkedList<>();
            while (resultSet.next()) {
                String schema = resultSet.getString(1);
                schemas.add(schema);
            }

            for (String schema : schemas) {
                statement.executeUpdate(String.format("DROP SCHEMA IF EXISTS \"%s\" CASCADE", schema));
            }
        }
        try (Statement statement = getConnection().createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
        ) {
            List<String> tables = new LinkedList<>();
            while (resultSet.next()) {
                String table = resultSet.getString(1);
                tables.add(table);
            }
            for (String table : tables) {
                statement.executeUpdate(String.format("DROP TABLE IF EXISTS \"public\".\"%s\" CASCADE", table));
            }
        }
    }

    public int getNumEnvironments() {
        return mockEnvironments.size();
    }

    public List<MockEnvironment> getMockEnvironments() {
        return mockEnvironments;
    }

    public int getWaitForDataPropagationTime() {
        return 500 + (Objects.equals(System.getenv("GITHUB_ACTIONS"), "true") ? 1000 : 0);
    }

    public int getWaitForUpdateHandlersTime() {
        return 100 + (Objects.equals(System.getenv("GITHUB_ACTIONS"), "true") ? 500 : 0);
    }

    public void waitForUpdateHandlers() {
        try {
            Thread.sleep(getWaitForUpdateHandlersTime());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void waitForDataPropagation() {
        try {
            Thread.sleep(getWaitForDataPropagationTime());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public Connection getH2Connection(DataManager dataManager) {
        Connection h2Connection;
        try {
            Method getConnectionMethod = H2DataAccessor.class.getDeclaredMethod("getConnection");
            getConnectionMethod.setAccessible(true);
            h2Connection = (Connection) getConnectionMethod.invoke(dataManager.getDataAccessor());
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        return h2Connection;
    }
}
