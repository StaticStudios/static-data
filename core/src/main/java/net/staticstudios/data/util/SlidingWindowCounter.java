package net.staticstudios.data.util;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

public class SlidingWindowCounter {
    private final long windowNanos;
    private final int bucketCount;
    private final long bucketDurationNanos;
    private final LongAdder[] buckets;
    private final AtomicLong currentBucketIndex = new AtomicLong(0);
    private final long startNanos;

    public SlidingWindowCounter(long windowMillis, int bucketCount) {
        this.windowNanos = windowMillis * 1_000_000L;
        this.bucketCount = bucketCount;
        this.bucketDurationNanos = windowNanos / bucketCount;
        this.buckets = new LongAdder[bucketCount];
        for (int i = 0; i < bucketCount; i++) {
            buckets[i] = new LongAdder();
        }
        this.startNanos = System.nanoTime();
        this.currentBucketIndex.set(0);
    }

    public void increment() {
        long now = System.nanoTime();
        long idx = (now - startNanos) / bucketDurationNanos;
        advance(idx);
        buckets[(int) (idx % bucketCount)].increment();
    }

    public double getPerSecond() {
        long now = System.nanoTime();
        long idx = (now - startNanos) / bucketDurationNanos;
        advance(idx);

        long total = 0;
        long oldestActiveIdx = idx - bucketCount + 1;
        for (int i = 0; i < bucketCount; i++) {
            long bucketIdx = oldestActiveIdx + i;
            if (bucketIdx >= 0 && bucketIdx <= idx) {
                total += buckets[(int) (bucketIdx % bucketCount)].sum();
            }
        }

        double windowSeconds = windowNanos / 1_000_000_000.0;
        return total / windowSeconds;
    }

    private void advance(long targetIdx) {
        long prev;
        while ((prev = currentBucketIndex.get()) < targetIdx) {
            if (!currentBucketIndex.compareAndSet(prev, prev + 1)) {
                continue;
            }
            long toClear = prev + 1;
            if (toClear <= targetIdx) {
                buckets[(int) (toClear % bucketCount)].reset();
            }
        }
    }
}

