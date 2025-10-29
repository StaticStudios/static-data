package net.staticstudios.data.benchmark;

import org.openjdk.jmh.annotations.*;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Warmup(iterations = 3)
@Measurement(iterations = 10)
public class StaticDataBenchmark {

    @Benchmark
    public void sampleBenchmark(StaticDataBenchmarkState state) {
        // Sample benchmark method
        int sum = 0;
        for (int i = 0; i < 1000; i++) {
            sum += i;
        }
    }

    @Benchmark
    public void testPersistentValueRead(StaticDataBenchmarkState state) {

    }

    @Benchmark
    public void testUniqueDataInsertAsync(StaticDataBenchmarkState state) {
    }

    @Benchmark
    public void testPersistentValueWrite(StaticDataBenchmarkState state) {
    }
}
