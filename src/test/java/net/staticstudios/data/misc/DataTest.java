package net.staticstudios.data.misc;

import com.redis.testcontainers.RedisContainer;
import net.staticstudios.data.DataManager;
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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

public class DataTest {
    public static final int NUM_ENVIRONMENTS = 1;
    public static RedisContainer redis;
    public static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            "postgres:16.2"
    );
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
        DataManager dataManager = new DataManager(dataSourceConfig);

        MockEnvironment mockEnvironment = new MockEnvironment(dataSourceConfig, dataManager);
        mockEnvironments.add(mockEnvironment);
        return mockEnvironment;
    }

    @AfterEach
    public void teardownThreadUtils() {
        ThreadUtils.shutdown();
    }

    public int getNumEnvironments() {
        return mockEnvironments.size();
    }

    public List<MockEnvironment> getMockEnvironments() {
        return mockEnvironments;
    }

    public void waitForDataPropagation() {
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        if (Objects.equals(System.getenv("GITHUB_ACTIONS"), "true")) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

}
