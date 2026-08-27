package net.staticstudios.data.benchmark;

import net.staticstudios.data.benchmark.data.SkyblockPlayer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;

/**
 * Reports achieved operations per second for production read paths. Each
 * benchmark defaults to eight concurrent reader threads; use -PjmhThreads to
 * build a 1/4/8/16/32-thread scaling curve.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Threads(8)
@Fork(1)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
public class ReadThroughputBenchmark {

    /** One real DataManager instance-cache lookup, including identifier-key construction. */
    @Benchmark
    public SkyblockPlayer instanceCacheHit(ReadThroughputState state) {
        return state.readInstance();
    }

    /** One player lookup followed by one PersistentValue read. */
    @Benchmark
    public long persistentValueRead(ReadThroughputState state) {
        return state.readPersistentValue();
    }

    /** One player lookup followed by settings-reference resolution. */
    @Benchmark
    public long referenceRead(ReadThroughputState state) {
        return state.readReference();
    }

    /** One player lookup and one uncached many-to-many H2 membership query. */
    @Benchmark
    public long friendCollectionRead(ReadThroughputState state) {
        return state.readFriendCollection();
    }

    /** Player lookup, settings reference, priority value, and player-name value. */
    @Benchmark
    public long compoundPlayerRead(ReadThroughputState state) {
        return state.readRandomPlayer();
    }

    /** A full configurable online-player scan, including settings and friends. */
    @Benchmark
    public long completePlayerScan(ReadThroughputState state) {
        return state.runMinecraftTick();
    }
}
