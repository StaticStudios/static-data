package net.staticstudios.data.util;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.pool.HikariPool;
import net.staticstudios.utils.ShutdownStage;
import net.staticstudios.utils.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.sql.Connection;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class TaskQueue {
    private static final Logger LOGGER = LoggerFactory.getLogger(TaskQueue.class);
    private final BlockingDeque<ConnectionJedisConsumer> taskQueue = new LinkedBlockingDeque<>();
    private final AtomicBoolean isShutdown = new AtomicBoolean(false);
    private final ExecutorService executor;
    private final HikariPool connectionPool;
    private final JedisPool jedisPool;

    public TaskQueue(DataSourceConfig config, String applicationName) {
        HikariConfig poolConfig = new HikariConfig();
        poolConfig.setDataSourceClassName("com.impossibl.postgres.jdbc.PGDataSource");
        poolConfig.addDataSourceProperty("serverName", config.databaseHost());
        poolConfig.addDataSourceProperty("portNumber", config.databasePort());
        poolConfig.addDataSourceProperty("user", config.databaseUsername());
        poolConfig.addDataSourceProperty("password", config.databasePassword());
        poolConfig.addDataSourceProperty("databaseName", config.databaseName());
        poolConfig.addDataSourceProperty("ApplicationName", applicationName);
        poolConfig.setLeakDetectionThreshold(10000);
        poolConfig.setMaximumPoolSize(1);

        this.connectionPool = new HikariPool(poolConfig);
        this.jedisPool = new JedisPool(config.redisHost(), config.redisPort());
        this.jedisPool.setMaxTotal(1);

        executor = Executors.newSingleThreadExecutor(r -> {
            Thread thread = new Thread(r);
            thread.setName("SQLTaskQueue");
            thread.setDaemon(true);
            return thread;
        });

        start();

        ThreadUtils.onShutdownRunSync(ShutdownStage.CLEANUP, this::shutdown);
    }

    public CompletableFuture<Void> submitTask(ConnectionConsumer task) {
        return submitTask((connection, jedis) -> task.accept(connection));
    }

    public CompletableFuture<Void> submitTask(ConnectionJedisConsumer task) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        taskQueue.addLast((connection, jedis) -> {
            try {
                task.accept(connection, jedis);
                future.complete(null);
            } catch (Exception e) {
                LOGGER.error("Error executing task in TaskQueue", e);
                future.completeExceptionally(e);
            }
        });
        return future;
    }

    private void start() {
        executor.submit(() -> {
            while (!(isShutdown.get() && taskQueue.isEmpty())) {
                ConnectionJedisConsumer task;
                try {
                    task = taskQueue.takeFirst();
                } catch (InterruptedException e) {
                    // We're shutting down
                    break;
                }

                try (
                        Connection connection = connectionPool.getConnection();
                        Jedis jedis = jedisPool.getResource()
                ) {
                    task.accept(connection, jedis);

                    if (!connection.getAutoCommit()) {
                        connection.setAutoCommit(true);
                    }
                } catch (Exception e) {
                    LOGGER.error("Error executing task in TaskQueue", e);
                }
            }
        });
    }

    private void shutdown() {
        if (!isShutdown.compareAndSet(false, true)) {
            return;
        }
        executor.shutdown();

        if (taskQueue.isEmpty()) {
            executor.shutdownNow();
        }

        try {
            if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            LOGGER.error("Interrupted while shutting down TaskQueue", e);
        }
    }
}
