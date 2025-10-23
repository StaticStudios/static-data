package net.staticstudios.data.benchmark;

import com.redis.testcontainers.RedisContainer;
import net.staticstudios.data.DataManager;
import net.staticstudios.data.benchmark.data.SkyblockPlayer;
import net.staticstudios.data.util.DataSourceConfig;
import net.staticstudios.utils.ThreadUtilProvider;
import net.staticstudios.utils.ThreadUtils;
import org.openjdk.jmh.annotations.*;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

@State(Scope.Benchmark)
public class StaticDataBenchmarkState {
    public static RedisContainer redis;
    private static PostgreSQLContainer<?> postgres;

    @Setup(Level.Trial)
    public void setup() throws Exception {
        if (postgres == null) {
            redis = new RedisContainer(DockerImageName.parse("redis:6.2.6"));
            redis.start();

            redis.execInContainer("redis-cli", "config", "set", "notify-keyspace-events", "KEA");

            postgres = new PostgreSQLContainer<>("postgres:16.2")
                    .withExposedPorts(5432)
                    .withPassword("password")
                    .withUsername("postgres")
                    .withDatabaseName("postgres");
            postgres.start();

            ThreadUtils.setProvider(ThreadUtilProvider.builder().build());
            DataSourceConfig dataSourceConfig = new DataSourceConfig(
                    postgres.getHost(),
                    postgres.getFirstMappedPort(),
                    postgres.getDatabaseName(),
                    postgres.getUsername(),
                    postgres.getPassword(),
                    redis.getHost(),
                    redis.getRedisPort());

            DataManager dataManager = new DataManager(dataSourceConfig, true);
            dataManager.load(SkyblockPlayer.class);
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        ThreadUtils.shutdown();
        if (postgres != null) {
            postgres.stop();
        }

        if (redis != null) {
            redis.stop();
        }
    }
}
