package net.staticstudios.data.benchmark;

import com.redis.testcontainers.RedisContainer;
import net.staticstudios.data.DataManager;
import net.staticstudios.data.InsertMode;
import net.staticstudios.data.StaticDataConfig;
import net.staticstudios.data.benchmark.data.SkyblockPlayer;
import net.staticstudios.data.benchmark.data.SkyblockPlayerSettings;
import net.staticstudios.data.insert.BatchInsert;
import net.staticstudios.data.util.ColumnValuePair;
import net.staticstudios.utils.ThreadUtilProvider;
import net.staticstudios.utils.ThreadUtils;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Owns the container-backed player dataset and read operations shared by the
 * throughput and distributed-load benchmarks.
 */
@State(Scope.Benchmark)
public abstract class PlayerWorkloadState {
    private static final int FRIENDS_PER_PLAYER = 8;

    /** Total rows in the benchmark dataset. */
    @Param({"100"})
    public int playerCount;

    /** Player working set used by round-robin read operations. */
    @Param({"100"})
    public int hotPlayerCount;

    /** Players scanned by one simulated server-tick operation. */
    @Param({"100"})
    public int playersPerTick;

    /** Retain and prewarm the complete random-read working set. */
    @Param({"true"})
    public boolean warmReadWorkingSet;

    private RedisContainer redis;
    private PostgreSQLContainer<?> postgres;
    private DataManager dataManager;
    private List<UUID> playerIds;
    private List<UUID> settingsIds;
    private List<SkyblockPlayer> retainedPlayers;
    private List<SkyblockPlayerSettings> retainedSettings;
    private final AtomicInteger nextPlayerIndex = new AtomicInteger();

    @Setup(Level.Trial)
    public void setup() throws Exception {
        validateParameters();

        redis = new RedisContainer(DockerImageName.parse("redis:7.4.1"));
        redis.start();
        redis.execInContainer("redis-cli", "config", "set", "notify-keyspace-events", "KEA");

        postgres = new PostgreSQLContainer<>("postgres:16.2")
                .withPassword("password")
                .withUsername("postgres")
                .withDatabaseName("postgres");
        postgres.start();

        ThreadUtils.setProvider(ThreadUtilProvider.builder().build());
        StaticDataConfig config = new StaticDataConfig(
                postgres.getHost(),
                postgres.getFirstMappedPort(),
                postgres.getDatabaseName(),
                postgres.getUsername(),
                postgres.getPassword(),
                redis.getHost(),
                redis.getRedisPort(),
                Runnable::run
        );

        dataManager = new DataManager(config, false);
        dataManager.load(SkyblockPlayer.class);
        dataManager.finishLoading();

        seedPlayers();
        warmLocalCaches();
        afterSetup();
    }

    /** Hook for specialized workloads that need resources after the dataset is ready. */
    protected void afterSetup() throws Exception {
    }

    private void validateParameters() {
        if (playerCount <= 0) {
            throw new IllegalArgumentException("playerCount must be positive");
        }
        if (hotPlayerCount <= 0 || hotPlayerCount > playerCount) {
            throw new IllegalArgumentException("hotPlayerCount must be between 1 and playerCount");
        }
        if (playersPerTick <= 0 || playersPerTick > playerCount) {
            throw new IllegalArgumentException("playersPerTick must be between 1 and playerCount");
        }
    }

    private void seedPlayers() {
        List<UUID> mutablePlayerIds = new ArrayList<>(playerCount);
        List<UUID> mutableSettingsIds = new ArrayList<>(playerCount);
        List<CompletableFuture<SkyblockPlayer>> playerFutures = new ArrayList<>(playerCount);
        List<CompletableFuture<SkyblockPlayerSettings>> settingsFutures = new ArrayList<>(playerCount);
        BatchInsert batch = dataManager.createBatchInsert();

        for (int i = 0; i < playerCount; i++) {
            UUID settingsId = UUID.randomUUID();
            UUID playerId = UUID.randomUUID();

            settingsFutures.add(SkyblockPlayerSettings.builder(dataManager)
                    .id(settingsId)
                    .tablistPriority(i)
                    .insert(batch));
            playerFutures.add(SkyblockPlayer.builder(dataManager)
                    .id(playerId)
                    .name("FakePlayer" + i)
                    .settingsId(settingsId)
                    .insert(batch));

            mutableSettingsIds.add(settingsId);
            mutablePlayerIds.add(playerId);
        }

        batch.insert(InsertMode.SYNC);

        List<SkyblockPlayer> allPlayers = playerFutures.stream().map(CompletableFuture::join).toList();
        List<SkyblockPlayerSettings> allSettings = settingsFutures.stream().map(CompletableFuture::join).toList();

        int friendsPerPlayer = Math.min(FRIENDS_PER_PLAYER, playerCount - 1);
        for (int playerIndex = 0; playerIndex < allPlayers.size(); playerIndex++) {
            List<SkyblockPlayer> friends = new ArrayList<>(friendsPerPlayer);
            for (int offset = 1; offset <= friendsPerPlayer; offset++) {
                friends.add(allPlayers.get((playerIndex + offset) % allPlayers.size()));
            }
            allPlayers.get(playerIndex).friends.addAll(friends);
        }

        dataManager.flushTaskQueue();
        playerIds = Collections.unmodifiableList(mutablePlayerIds);
        settingsIds = Collections.unmodifiableList(mutableSettingsIds);

        int retainedPlayerCount = Math.min(
                playerCount,
                Math.max(32, Math.max(playersPerTick, warmReadWorkingSet ? hotPlayerCount : 0))
        );
        retainedPlayers = Collections.unmodifiableList(
                new ArrayList<>(allPlayers.subList(0, retainedPlayerCount))
        );
        retainedSettings = Collections.unmodifiableList(
                new ArrayList<>(allSettings.subList(0, retainedPlayerCount))
        );
    }

    private void warmLocalCaches() {
        int playersToWarm = Math.max(playersPerTick, warmReadWorkingSet ? hotPlayerCount : 0);
        for (int i = 0; i < playersToWarm; i++) {
            SkyblockPlayer player = playerAt(i);
            player.name.get();
            player.settings.get();
            settingsAt(i).tablistPriority.get();
            if (i < playersPerTick) {
                player.friends.forEach(friend -> friend.name.get());
            }
        }
    }

    public long runMinecraftTick() {
        long checksum = 0;

        for (int i = 0; i < playersPerTick; i++) {
            SkyblockPlayer player = playerAt(i);
            checksum += player.name.get().hashCode();

            SkyblockPlayerSettings playerSettings = player.settings.get();
            if (playerSettings != null) {
                checksum += playerSettings.tablistPriority.get();
            }
            for (SkyblockPlayer friend : player.friends) {
                checksum += friend.name.get().hashCode();
            }
        }

        return checksum;
    }

    public long readRandomPlayer() {
        SkyblockPlayer player = playerAt(nextReadPlayerIndex());
        SkyblockPlayerSettings playerSettings = player.settings.get();
        int priority = playerSettings == null ? 0 : playerSettings.tablistPriority.get();
        return 31L * player.name.get().hashCode() + priority;
    }

    public SkyblockPlayer readInstance() {
        return playerAt(nextReadPlayerIndex());
    }

    public long readPersistentValue() {
        return playerAt(nextReadPlayerIndex()).name.get().hashCode();
    }

    public long readReference() {
        SkyblockPlayerSettings playerSettings = playerAt(nextReadPlayerIndex()).settings.get();
        return playerSettings == null ? 0 : playerSettings.getIdColumns().hashCode();
    }

    public long readFriendCollection() {
        long checksum = 0;
        for (SkyblockPlayer friend : playerAt(nextReadPlayerIndex()).friends) {
            checksum += friend.getIdColumns().hashCode();
        }
        return checksum;
    }

    private int nextReadPlayerIndex() {
        return Math.floorMod(nextPlayerIndex.getAndIncrement(), hotPlayerCount);
    }

    protected final SkyblockPlayer playerAt(int index) {
        return dataManager.getInstance(
                SkyblockPlayer.class,
                new ColumnValuePair("id", playerIds.get(index))
        );
    }

    protected final SkyblockPlayerSettings settingsAt(int index) {
        return dataManager.getInstance(
                SkyblockPlayerSettings.class,
                new ColumnValuePair("id", settingsIds.get(index))
        );
    }

    protected final UUID playerIdAt(int index) {
        return playerIds.get(index);
    }

    protected final UUID settingsIdAt(int index) {
        return settingsIds.get(index);
    }

    protected final DataManager dataManager() {
        return dataManager;
    }

    protected final PostgreSQLContainer<?> postgres() {
        return postgres;
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        try {
            beforeTearDown();
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

    /** Hook for specialized workloads that must close resources before the containers stop. */
    protected void beforeTearDown() {
    }
}
