package net.staticstudios.data.benchmark;

import net.staticstudios.data.benchmark.data.SkyblockPlayer;
import net.staticstudios.data.benchmark.data.SkyblockPlayerSettings;
import net.staticstudios.data.util.ColumnValuePair;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Warmup;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * End-to-end benchmarks backed by the same H2/PostgreSQL/Redis stack used in production.
 *
 * <p>The grouped benchmark models one Minecraft server thread scanning all online players
 * while four worker threads repeatedly resolve cached entities. This is the contention
 * pattern visible in the supplied spark profile.</p>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
public class StaticDataBenchmark {

    @Benchmark
    @Group("hotInstanceCacheHit")
    @GroupThreads(1)
    public SkyblockPlayer hotInstanceCacheHit(StaticDataBenchmarkState state) {
        UUID id = state.nextPlayerId();
        return state.dataManager().getInstance(
                SkyblockPlayer.class,
                new ColumnValuePair("id", id)
        );
    }

    @Benchmark
    @Group("minecraftTickUnderAsyncLoad")
    @GroupThreads(1)
    public long minecraftServerThreadTick(StaticDataBenchmarkState state) {
        long checksum = 0;

        for (UUID playerId : state.playerIds()) {
            SkyblockPlayer player = state.dataManager().getInstance(
                    SkyblockPlayer.class,
                    new ColumnValuePair("id", playerId)
            );

            checksum += player.name.get().hashCode();

            SkyblockPlayerSettings settings = player.settings.get();
            if (settings != null) {
                checksum += settings.tablistPriority.get();
            }

            for (SkyblockPlayer friend : player.friends) {
                checksum += friend.name.get().hashCode();
            }
        }

        return checksum;
    }

    @Benchmark
    @Group("minecraftTickUnderAsyncLoad")
    @GroupThreads(4)
    public SkyblockPlayer asyncCacheReader(StaticDataBenchmarkState state) {
        UUID id = state.nextPlayerId();
        return state.dataManager().getInstance(
                SkyblockPlayer.class,
                new ColumnValuePair("id", id)
        );
    }
}
