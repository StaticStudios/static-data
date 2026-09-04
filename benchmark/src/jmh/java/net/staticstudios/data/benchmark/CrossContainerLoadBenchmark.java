package net.staticstudios.data.benchmark;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;

/**
 * Exercises one Static Data container while writes from other containers are
 * committed to the same PostgreSQL database.
 *
 * <p>The remote writers use persistent PostgreSQL sessions with distinct
 * application names. Consequently every write follows the production path:
 * PostgreSQL trigger, NOTIFY, Static Data listener, H2 mirror update, H2
 * invalidation trigger, and the next local read.</p>
 */
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
public class CrossContainerLoadBenchmark {

    @Benchmark
    @Group("localOnly")
    @GroupThreads(1)
    public long localOnlyMinecraftTick(CrossContainerLoadState state) {
        return state.runMinecraftTick();
    }

    @Benchmark
    @Group("localOnly")
    @GroupThreads(4)
    public long localOnlyAsyncPlayerRead(CrossContainerLoadState state) {
        return state.readRandomPlayer();
    }

    @Benchmark
    @Group("readHeavyCrossContainer")
    @GroupThreads(1)
    public long readHeavyMinecraftTick(CrossContainerLoadState state) {
        return state.runMinecraftTick();
    }

    @Benchmark
    @Group("readHeavyCrossContainer")
    @GroupThreads(4)
    public long readHeavyAsyncPlayerRead(CrossContainerLoadState state) {
        return state.readRandomPlayer();
    }

    @Benchmark
    @Group("readHeavyCrossContainer")
    @GroupThreads(1)
    public long readHeavyRemoteWriter(CrossContainerLoadState state) {
        return state.writeFromRemoteContainer(true, 1).sequence();
    }

    @Benchmark
    @Group("writeHeavyCrossContainer")
    @GroupThreads(1)
    public long writeHeavyMinecraftTick(CrossContainerLoadState state) {
        return state.runMinecraftTick();
    }

    @Benchmark
    @Group("writeHeavyCrossContainer")
    @GroupThreads(4)
    public long writeHeavyAsyncPlayerRead(CrossContainerLoadState state) {
        return state.readRandomPlayer();
    }

    @Benchmark
    @Group("writeHeavyCrossContainer")
    @GroupThreads(8)
    public long writeHeavyRemoteWriter(CrossContainerLoadState state) {
        return state.writeFromRemoteContainer(true, 8).sequence();
    }

    @Benchmark
    @Group("controlledReadLoad")
    @GroupThreads(1)
    public long controlledReadLoadMinecraftTick(CrossContainerLoadState state) {
        return state.runMinecraftTick();
    }

    @Benchmark
    @Group("controlledReadLoad")
    @GroupThreads(8)
    public long controlledReadLoadReader(CrossContainerLoadState state) {
        return state.readRandomPlayerAtControlledRate();
    }

    @Benchmark
    @Group("controlledMixedLoad")
    @GroupThreads(1)
    public long controlledMixedLoadMinecraftTick(CrossContainerLoadState state) {
        return state.runMinecraftTick();
    }

    @Benchmark
    @Group("controlledMixedLoad")
    @GroupThreads(8)
    public long controlledMixedLoadReader(CrossContainerLoadState state) {
        return state.readRandomPlayerAtControlledRate();
    }

    @Benchmark
    @Group("controlledMixedLoad")
    @GroupThreads(8)
    public long controlledMixedLoadRemoteWriter(CrossContainerLoadState state) {
        return state.writeFromRemoteContainer(true, 8).sequence();
    }

    /** Measures the persistent-database write latency from four peer containers. */
    @Benchmark
    @Group("remoteWriteRoundTrip")
    @GroupThreads(4)
    public long remoteWriteRoundTrip(CrossContainerLoadState state) {
        return state.writeFromRemoteContainer(false, 4).sequence();
    }

    /**
     * Measures request-to-visibility latency with four independent remote writers.
     * The operation completes only after the listening Static Data instance reads
     * the newly committed value from its invalidated local cache.
     */
    @Benchmark
    @Group("remoteUpdatePropagation")
    @GroupThreads(4)
    public long remoteUpdatePropagation(CrossContainerLoadState state) {
        CrossContainerLoadState.RemoteWrite write = state.writeFromRemoteContainer(false, 4);
        return state.awaitRemoteWrite(write);
    }
}
