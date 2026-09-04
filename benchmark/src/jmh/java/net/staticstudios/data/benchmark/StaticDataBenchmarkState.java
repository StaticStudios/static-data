package net.staticstudios.data.benchmark;

import com.redis.testcontainers.RedisContainer;
import net.staticstudios.data.DataManager;
import net.staticstudios.data.InsertMode;
import net.staticstudios.data.StaticDataConfig;
import net.staticstudios.data.benchmark.data.SkyblockPlayer;
import net.staticstudios.data.benchmark.data.SkyblockPlayerSettings;
import net.staticstudios.utils.ThreadUtilProvider;
import net.staticstudios.utils.ThreadUtils;
import org.openjdk.jmh.annotations.*;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

@State(Scope.Group)
public class StaticDataBenchmarkState {
    public static final int PLAYER_COUNT = 32;
    private static final int FRIENDS_PER_PLAYER = 8;

    private RedisContainer redis;
    private PostgreSQLContainer<?> postgres;
    private DataManager dataManager;
    private List<UUID> playerIds;
    private List<SkyblockPlayer> players;
    private final AtomicInteger nextPlayerIndex = new AtomicInteger();

    @Setup(Level.Trial)
    public void setup() throws Exception {
        redis = new RedisContainer(DockerImageName.parse("redis:7.4.1"));
        redis.start();
        redis.execInContainer("redis-cli", "config", "set", "notify-keyspace-events", "KEA");

        postgres = new PostgreSQLContainer<>("postgres:16.2")
                .withPassword("password")
                .withUsername("postgres")
                .withDatabaseName("postgres");
        postgres.start();

        ThreadUtils.setProvider(ThreadUtilProvider.builder().build());
        StaticDataConfig dataSourceConfig = new StaticDataConfig(
                postgres.getHost(),
                postgres.getFirstMappedPort(),
                postgres.getDatabaseName(),
                postgres.getUsername(),
                postgres.getPassword(),
                redis.getHost(),
                redis.getRedisPort(),
                Runnable::run
        );

        dataManager = new DataManager(dataSourceConfig, false);
        dataManager.load(SkyblockPlayer.class);
        dataManager.finishLoading();

        List<UUID> mutablePlayerIds = new ArrayList<>(PLAYER_COUNT);
        List<SkyblockPlayer> mutablePlayers = new ArrayList<>(PLAYER_COUNT);

        for (int i = 0; i < PLAYER_COUNT; i++) {
            UUID settingsId = UUID.randomUUID();
            SkyblockPlayerSettings.builder(dataManager)
                    .id(settingsId)
                    .tablistPriority(i)
                    .insert(InsertMode.SYNC);

            UUID playerId = UUID.randomUUID();
            SkyblockPlayer player = SkyblockPlayer.builder(dataManager)
                    .id(playerId)
                    .name("FakePlayer" + i)
                    .settingsId(settingsId)
                    .insert(InsertMode.SYNC);

            mutablePlayerIds.add(playerId);
            mutablePlayers.add(player);
        }

        for (int playerIndex = 0; playerIndex < mutablePlayers.size(); playerIndex++) {
            List<SkyblockPlayer> friends = new ArrayList<>(FRIENDS_PER_PLAYER);
            for (int offset = 1; offset <= FRIENDS_PER_PLAYER; offset++) {
                friends.add(mutablePlayers.get((playerIndex + offset) % mutablePlayers.size()));
            }
            mutablePlayers.get(playerIndex).friends.addAll(friends);
        }

        dataManager.flushTaskQueue();
        playerIds = Collections.unmodifiableList(mutablePlayerIds);
        players = Collections.unmodifiableList(mutablePlayers);

        // Populate relation and prepared-statement caches before JMH starts measuring.
        for (SkyblockPlayer player : players) {
            player.name.get();
            player.settings.get();
            player.friends.forEach(friend -> friend.name.get());
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        try {
            if (dataManager != null) {
                dataManager.flushTaskQueue();
            }
        } finally {
            ThreadUtils.shutdown();

            if (postgres != null) {
                postgres.stop();
            }

            if (redis != null) {
                redis.stop();
            }
        }
    }

    public DataManager dataManager() {
        return dataManager;
    }

    public List<UUID> playerIds() {
        return playerIds;
    }

    public UUID nextPlayerId() {
        int index = Math.floorMod(nextPlayerIndex.getAndIncrement(), playerIds.size());
        return playerIds.get(index);
    }
}
