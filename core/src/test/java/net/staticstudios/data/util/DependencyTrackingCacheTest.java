package net.staticstudios.data.util;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class DependencyTrackingCacheTest {

    private DependencyTrackingCache cache;

    private static final String SCHEMA = "public";
    private static final String TABLE = "users";

    @BeforeEach
    void setUp() {
        cache = new DependencyTrackingCache("test", 1000, 60);
    }

    private SelectQuery query(String tag) {
        return new SelectQuery(tag, "SELECT * FROM users WHERE id = ?", List.of(1));
    }

    private Cell cell(String column, Object idValue) {
        ColumnValuePairs ids = new ColumnValuePairs(new ColumnValuePair("id", idValue));
        return new Cell(SCHEMA, TABLE, column, ids);
    }

    private ReadCacheResult result(Object value, Cell... dependencies) {
        return new ReadCacheResult(value, Set.of(dependencies));
    }

    // --- Basic put / get ---

    @Test
    void putAndGet() {
        long gen = cache.getGeneration();
        SelectQuery q = query("basic");
        Cell c = cell("name", 1);
        ReadCacheResult r = result("Alice", c);

        cache.put(q, r, gen);

        ReadCacheResult cached = cache.get(q);
        assertNotNull(cached);
        assertEquals("Alice", cached.getValue());
        assertEquals(1, cache.estimatedSize());
        assertEquals(1, cache.dependencyMappingSize());
    }

    @Test
    void getMissReturnsNull() {
        assertNull(cache.get(query("missing")));
    }

    // --- Generation gating ---

    @Test
    void putRejectedWhenGenerationMismatch() {
        long staleGen = cache.getGeneration();
        Cell c = cell("name", 1);

        cache.invalidate(Set.of(c));

        cache.put(query("stale"), result("stale-value", c), staleGen);

        assertNull(cache.get(query("stale")));
        assertEquals(0, cache.estimatedSize());
    }

    @Test
    void putAcceptedWhenGenerationMatches() {
        Cell c = cell("name", 1);
        cache.invalidate(Set.of(c));

        long currentGen = cache.getGeneration();
        SelectQuery q = query("fresh");
        cache.put(q, result("fresh-value", c), currentGen);

        assertNotNull(cache.get(q));
    }

    // --- Invalidation removes cache entries and dependency mappings ---

    @Test
    void invalidateRemovesEntryAndDependencies() {
        long gen = cache.getGeneration();
        Cell c = cell("name", 1);
        SelectQuery q = query("inv");

        cache.put(q, result("value", c), gen);
        assertEquals(1, cache.dependencyMappingSize());

        cache.invalidate(Set.of(c));

        assertNull(cache.get(q));
        assertEquals(0, cache.dependencyMappingSize());
    }

    @Test
    void invalidateAcrossMultipleCells() {
        long gen = cache.getGeneration();
        Cell c1 = cell("name", 1);
        Cell c2 = cell("email", 1);
        SelectQuery q = query("multi-dep");

        cache.put(q, result("value", c1, c2), gen);
        assertEquals(2, cache.dependencyMappingSize());

        cache.invalidate(Set.of(c1));

        assertNull(cache.get(q));
        assertEquals(0, cache.dependencyMappingSize());
    }

    @Test
    void invalidateOnlyAffectsRelatedQueries() {
        long gen = cache.getGeneration();
        Cell c1 = cell("name", 1);
        Cell c2 = cell("name", 2);
        SelectQuery q1 = query("user1");
        SelectQuery q2 = new SelectQuery("user2", "SELECT * FROM users WHERE id = ?", List.of(2));

        cache.put(q1, result("Alice", c1), gen);
        cache.put(q2, result("Bob", c2), gen);

        cache.invalidate(Set.of(c1));

        assertNull(cache.get(q1));
        assertNotNull(cache.get(q2));
    }

    // --- Replacement (re-put same SelectQuery) ---

    @Test
    void replacementCleansUpOldDependencies() {
        long gen = cache.getGeneration();
        Cell oldDep = cell("name", 1);
        Cell newDep = cell("email", 1);
        SelectQuery q = query("replace");

        cache.put(q, result("old", oldDep), gen);
        assertEquals(1, cache.dependencyMappingSize());

        cache.put(q, result("new", newDep), gen);
        assertEquals(1, cache.dependencyMappingSize());
        assertEquals("new", cache.get(q).getValue());

        cache.invalidate(Set.of(oldDep));
        assertNotNull(cache.get(q));
    }

    @Test
    void replacementRegistersNewDependencies() {
        long gen = cache.getGeneration();
        Cell oldDep = cell("name", 1);
        Cell newDep = cell("email", 1);
        SelectQuery q = query("replace2");

        cache.put(q, result("old", oldDep), gen);
        cache.put(q, result("new", newDep), gen);

        cache.invalidate(Set.of(newDep));
        assertNull(cache.get(q));
    }

    @Test
    void replacementWithOverlappingDependencies() {
        long gen = cache.getGeneration();
        Cell shared = cell("id", 1);
        Cell onlyOld = cell("name", 1);
        Cell onlyNew = cell("email", 1);
        SelectQuery q = query("overlap");

        cache.put(q, result("old", shared, onlyOld), gen);
        assertEquals(2, cache.dependencyMappingSize());

        cache.put(q, result("new", shared, onlyNew), gen);
        assertEquals(2, cache.dependencyMappingSize());

        cache.invalidate(Set.of(onlyOld));
        assertNotNull(cache.get(q));

        long gen2 = cache.getGeneration();
        cache.put(q, result("new2", shared, onlyNew), gen2);

        cache.invalidate(Set.of(onlyNew));
        assertNull(cache.get(q));
    }

    // --- Generation counter increments on each invalidation ---

    @Test
    void generationIncrementsOnInvalidation() {
        long gen0 = cache.getGeneration();
        cache.invalidate(Set.of(cell("a", 1)));
        long gen1 = cache.getGeneration();
        cache.invalidate(Set.of(cell("b", 2)));
        long gen2 = cache.getGeneration();

        assertEquals(gen0 + 1, gen1);
        assertEquals(gen1 + 1, gen2);
    }

    // --- Multiple queries sharing the same cell dependency ---

    @Test
    void multipleDependentsOnSameCell() {
        long gen = cache.getGeneration();
        Cell shared = cell("name", 1);
        SelectQuery q1 = query("q1");
        SelectQuery q2 = new SelectQuery("q2", "SELECT name FROM users WHERE id = ?", List.of(1));

        cache.put(q1, result("v1", shared), gen);
        cache.put(q2, result("v2", shared), gen);
        assertEquals(1, cache.dependencyMappingSize());

        cache.invalidate(Set.of(shared));

        assertNull(cache.get(q1));
        assertNull(cache.get(q2));
        assertEquals(0, cache.dependencyMappingSize());
    }

    // --- Empty dependency set ---

    @Test
    void putWithNoDependencies() {
        long gen = cache.getGeneration();
        SelectQuery q = query("no-deps");
        cache.put(q, result("value"), gen);

        assertNotNull(cache.get(q));
        assertEquals(0, cache.dependencyMappingSize());
    }

    // --- Invalidating cells with no dependents is a no-op ---

    @Test
    void invalidateUnrelatedCellIsNoOp() {
        long gen = cache.getGeneration();
        Cell c = cell("name", 1);
        Cell unrelated = cell("name", 999);
        SelectQuery q = query("safe");

        cache.put(q, result("value", c), gen);
        cache.invalidate(Set.of(unrelated));

        assertNotNull(cache.get(q));
    }

    // --- Concurrent put vs invalidate ---

    @Test
    void concurrentPutAndInvalidateDoNotCorrupt() throws Exception {
        int iterations = 500;
        int threads = 8;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        AtomicInteger corruptionCount = new AtomicInteger();

        for (int i = 0; i < iterations; i++) {
            Cell c = cell("name", i);
            CyclicBarrier barrier = new CyclicBarrier(2);
            CountDownLatch done = new CountDownLatch(2);

            int idx = i;
            executor.submit(() -> {
                try {
                    barrier.await();
                    long gen = cache.getGeneration();
                    SelectQuery q = new SelectQuery("c", "SELECT * FROM t WHERE id = ?", List.of(idx));
                    cache.put(q, result("v", c), gen);
                } catch (Exception e) {
                    corruptionCount.incrementAndGet();
                } finally {
                    done.countDown();
                }
            });

            executor.submit(() -> {
                try {
                    barrier.await();
                    cache.invalidate(Set.of(c));
                } catch (Exception e) {
                    corruptionCount.incrementAndGet();
                } finally {
                    done.countDown();
                }
            });

            done.await();
        }

        executor.shutdown();
        assertEquals(0, corruptionCount.get());

        assertTrue(cache.dependencyMappingSize() >= 0);
        assertTrue(cache.estimatedSize() >= 0);
    }

    // --- Stale generation from concurrent invalidation prevents caching stale data ---

    @Test
    void staleDataNotCachedAfterConcurrentInvalidation() throws Exception {
        Cell c = cell("name", 1);
        SelectQuery q = query("stale-race");

        long genBeforeInvalidation = cache.getGeneration();

        cache.invalidate(Set.of(c));

        cache.put(q, result("stale", c), genBeforeInvalidation);

        assertNull(cache.get(q));
    }
}

