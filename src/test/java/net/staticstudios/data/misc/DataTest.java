package net.staticstudios.data.misc;

import com.redis.testcontainers.RedisContainer;
import com.zaxxer.hikari.HikariConfig;
import net.staticstudios.data.DataManager;
import net.staticstudios.utils.JedisProvider;
import net.staticstudios.utils.ThreadUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

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
    public static HikariConfig hikariConfig;
    private static Connection connection;
    private static Jedis jedis;
    private List<MockEnvironment> mockEnvironments;

    @BeforeAll
    static void initPostgres() throws IOException, SQLException, InterruptedException {
        postgres.start();
        redis = new RedisContainer(DockerImageName.parse("redis:6.2.6"));
        redis.start();

        redis.execInContainer("redis-cli", "config", "set", "notify-keyspace-events", "KEA");

        hikariConfig = new HikariConfig();
        hikariConfig.setDataSourceClassName("com.impossibl.postgres.jdbc.PGDataSource");
        hikariConfig.addDataSourceProperty("serverName", postgres.getHost());
        hikariConfig.addDataSourceProperty("portNumber", postgres.getFirstMappedPort());
        hikariConfig.addDataSourceProperty("user", postgres.getUsername());
        hikariConfig.addDataSourceProperty("password", postgres.getPassword());
        hikariConfig.addDataSourceProperty("databaseName", postgres.getDatabaseName());
        hikariConfig.setLeakDetectionThreshold(10000);
        hikariConfig.setMaximumPoolSize(10);
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

            DataManager dataManager = new DataManager(hikariConfig, new JedisProvider() {
                private final JedisPool jedisPool = new JedisPool(redis.getHost(), redis.getRedisPort());

                @Override
                public Jedis getJedis() {
                    return jedisPool.getResource();
                }

                @Override
                public String getJedisHost() {
                    return redis.getHost();
                }

                @Override
                public int getJedisPort() {
                    return redis.getRedisPort();
                }
            });

            MockEnvironment mockEnvironment = new MockEnvironment(hikariConfig, dataManager);
            mockEnvironments.add(mockEnvironment);
        }
    }

    @AfterEach
    public void teardownThreadUtils() {
        ThreadUtils.shutdown();
    }

    public int getNumEnvironments() {
        return NUM_ENVIRONMENTS;
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
