package net.staticstudios.data.util;

import com.github.fppt.jedismock.RedisServer;
import com.zaxxer.hikari.HikariConfig;
import net.staticstudios.data.mocks.MockThreadProvider;
import net.staticstudios.messaging.Messenger;
import net.staticstudios.messaging.PingMessage;
import net.staticstudios.messaging.PingMessageHandler;
import net.staticstudios.utils.JedisProvider;
import net.staticstudios.utils.ThreadUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.PostgreSQLContainer;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Base test class for data tests.
 * This class sets up a postgres container and a redis server for testing.
 * It also provides a method to block the current thread until all messages have been propagated.
 */
public class DataTest {

    //I tried using test containers for redis but ran into a lot of issues. This should be good enough for testing.
    public static RedisServer redis;
    public static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            "postgres:16.2"
    );
    public static Connection connection;
    public static HikariConfig hikariConfig;
    private static Messenger propagationMessenger;

    @BeforeAll
    static void init() throws IOException, SQLException {
        postgres.start();
        redis = RedisServer.newRedisServer().start();


        hikariConfig = new HikariConfig();
        hikariConfig.setDataSourceClassName("org.postgresql.ds.PGSimpleDataSource");
        hikariConfig.addDataSourceProperty("serverName", postgres.getHost());
        hikariConfig.addDataSourceProperty("portNumber", postgres.getFirstMappedPort());
        hikariConfig.addDataSourceProperty("user", postgres.getUsername());
        hikariConfig.addDataSourceProperty("password", postgres.getPassword());
        hikariConfig.setLeakDetectionThreshold(10000);

        connection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
    }

    @BeforeEach
    final void setPropagationMessenger() {
        ThreadUtils.setProvider(new MockThreadProvider());

        propagationMessenger = new Messenger("propagation", new JedisProvider() {
            private final JedisPool jedisPool = new JedisPool(redis.getHost(), redis.getBindPort());

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
                return redis.getBindPort();
            }
        });
    }

    @AfterEach
    final void shutdownThreadProvider() {
        MockThreadProvider threadProvider = (MockThreadProvider) ThreadUtils.provider;
        threadProvider.shutdown();
    }

    /**
     * Blocks the current thread until all messages have been propagated.
     */
    protected void waitForDataPropagation() {
        //Ping myself to ensure that redis has had enough time to handle every queued message

        AtomicBoolean propagated = new AtomicBoolean(false);
        propagationMessenger.sendMessage("propagation", PingMessageHandler.class, new PingMessage()).thenRun(() -> propagated.set(true));
        while (!propagated.get()) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            propagated.set(true);
        }
    }
}
