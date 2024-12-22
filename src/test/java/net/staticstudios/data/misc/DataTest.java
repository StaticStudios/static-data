package net.staticstudios.data.misc;

import com.zaxxer.hikari.HikariConfig;
import net.staticstudios.data.DataManager;
import net.staticstudios.data.impl.pg.PostgresListener;
import net.staticstudios.utils.Pair;
import net.staticstudios.utils.ThreadUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class DataTest {
    public static final int NUM_ENVIRONMENTS = 1;
    public static boolean USE_MOCK_DATABASE = true; //todo: this should be set in an env file
    public static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            "postgres:16.2"
    );
    public static HikariConfig hikariConfig;
    public static Connection connection;
    private final Logger dataPropogationlogger = LoggerFactory.getLogger("WaitForDataPropagation");
    private final Map<UUID, Pair<AtomicInteger, CompletableFuture<Void>>> waitForDataPropagationCallbacks = new ConcurrentHashMap<>();
    private List<MockEnvironment> mockEnvironments;

    @BeforeAll
    static void initPostgres() throws IOException, SQLException {
        postgres.start();
//        redis = RedisServer.newRedisServer().start();

        hikariConfig = new HikariConfig();
        if (USE_MOCK_DATABASE) {
            hikariConfig.setDataSourceClassName("org.postgresql.ds.PGSimpleDataSource");
            hikariConfig.addDataSourceProperty("serverName", postgres.getHost());
            hikariConfig.addDataSourceProperty("portNumber", postgres.getFirstMappedPort());
            hikariConfig.addDataSourceProperty("user", postgres.getUsername());
            hikariConfig.addDataSourceProperty("password", postgres.getPassword());
            hikariConfig.addDataSourceProperty("databaseName", postgres.getDatabaseName());
            hikariConfig.setLeakDetectionThreshold(10000);
            hikariConfig.setMaximumPoolSize(10);
            connection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
        } else {
            //todo: we should have these set in an env file
            hikariConfig.setDataSourceClassName("com.impossibl.postgres.jdbc.PGDataSource");
            hikariConfig.addDataSourceProperty("serverName", "localhost");
            hikariConfig.addDataSourceProperty("portNumber", 12345);
            hikariConfig.addDataSourceProperty("user", "postgres");
            hikariConfig.addDataSourceProperty("password", "password");
            hikariConfig.addDataSourceProperty("databaseName", "postgres");
            hikariConfig.setLeakDetectionThreshold(10000);
            hikariConfig.setMaximumPoolSize(10);

            connection = DriverManager.getConnection("jdbc:pgsql://localhost:12345/postgres", "postgres", "password");
        }

        try (Statement statement = connection.createStatement()) {
            statement.execute("DROP SCHEMA IF EXISTS wait_for_data_propagation_callback CASCADE");
            statement.execute("CREATE SCHEMA wait_for_data_propagation_callback");
            statement.execute("CREATE TABLE wait_for_data_propagation_callback.callback (id UUID PRIMARY KEY)");
        }
    }

    @AfterAll
    public static void cleanup() {
        try (Statement statement = connection.createStatement()) {
            statement.execute("DROP SCHEMA wait_for_data_propagation_callback CASCADE");
        } catch (SQLException e) {
            e.printStackTrace();
        }


        postgres.stop();
    }

    public static Connection getConnection() {
        return connection;
    }

    @BeforeEach
    public void setupMockEnvironments() {
        mockEnvironments = new LinkedList<>();
        ThreadUtils.setProvider(new MockThreadProvider());
        for (int i = 0; i < NUM_ENVIRONMENTS; i++) {

            DataManager dataManager = new DataManager(hikariConfig);

            MockEnvironment mockEnvironment = new MockEnvironment(hikariConfig, dataManager);
            mockEnvironments.add(mockEnvironment);
        }

        //Wait for data propagation callback to be received on every environment
        mockEnvironments.forEach(env -> {
            PostgresListener pgListener = env.dataManager().getPostgresListener();

            pgListener.ensureTableHasTrigger(connection, "wait_for_data_propagation_callback.callback");

            pgListener.addHandler(notif -> {
                if (notif.getSchema().equals("wait_for_data_propagation_callback")) {
                    UUID id = UUID.fromString(notif.getData().newDataValueMap().get("id"));
                    Pair<AtomicInteger, CompletableFuture<Void>> pair = waitForDataPropagationCallbacks.get(id);
                    if (pair != null) {
                        int callbacksLeftToReceive = pair.first().decrementAndGet();
                        if (callbacksLeftToReceive == 0) {
                            pair.second().complete(null);
                        }
                    }
                }
            });
        });
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
//        long start = System.currentTimeMillis();
//        UUID callbackId = UUID.randomUUID();
//        dataPropogationlogger.info("Waiting for data propagation (callback id: {})", callbackId);
//        AtomicInteger callbacksLeftToReceive = new AtomicInteger(NUM_ENVIRONMENTS);
//        CompletableFuture<Void> future = new CompletableFuture<>();
//        waitForDataPropagationCallbacks.put(callbackId, Pair.of(callbacksLeftToReceive, future));
//        try (Statement statement = connection.createStatement()) {
//            statement.execute("INSERT INTO wait_for_data_propagation_callback.callback (id) VALUES ('" + callbackId + "')");
//        } catch (SQLException e) {
//            throw new RuntimeException(e);
//        }
//
//        future.join();
//        waitForDataPropagationCallbacks.remove(callbackId);
//        long end = System.currentTimeMillis();
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
//        dataPropogationlogger.info("Data propagation complete (callback id: {}) in {}ms", callbackId, end - start);
    }

}
