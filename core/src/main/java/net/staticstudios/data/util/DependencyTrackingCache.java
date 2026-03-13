package net.staticstudios.data.util;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DependencyTrackingCache {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private final String name;
    private final Cache<SelectQuery, ReadCacheResult> cache;
    private final Map<Cell, Set<SelectQuery>> dependencyMapping = new ConcurrentHashMap<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final AtomicLong generation = new AtomicLong();

    public DependencyTrackingCache(String name, long maximumSize, long expireAfterWriteMinutes) {
        this.name = name;
        this.cache = Caffeine.newBuilder()
                .maximumSize(maximumSize)
                .expireAfterWrite(expireAfterWriteMinutes, TimeUnit.MINUTES)
                .removalListener((SelectQuery query, ReadCacheResult result, RemovalCause cause) -> {
                    if (query != null && result != null && cause != RemovalCause.REPLACED) {
                        cleanup(query, result);
                    }
                })
                .executor(Runnable::run)
                .build();
    }

    public @Nullable ReadCacheResult get(SelectQuery query) {
        return cache.getIfPresent(query);
    }

    public long getGeneration() {
        return generation.get();
    }

    public void put(SelectQuery query, @NotNull ReadCacheResult result, long expectedGeneration) {
        lock.readLock().lock();
        try {
            if (generation.get() != expectedGeneration) {
                return;
            }
            logger.trace("Putting result in {} cache for query {} with result {}", name, query, result);

            // If there's an existing cached result for this query, clean up its dependencies first
            ReadCacheResult previous = cache.getIfPresent(query);
            if (previous != null) {
                cleanup(query, previous);
            }

            for (Cell cell : result.getDependencies()) {
                dependencyMapping.computeIfAbsent(cell, k -> ConcurrentHashMap.newKeySet())
                        .add(query);
            }
            cache.put(query, result);
        } finally {
            lock.readLock().unlock();
        }
    }

    public void invalidate(Set<Cell> cells) {
        lock.writeLock().lock();
        try {
            generation.incrementAndGet();
            for (Cell cell : cells) {
                Set<SelectQuery> queries = dependencyMapping.remove(cell);
                if (queries != null) {
                    for (SelectQuery query : queries) {
                        cache.invalidate(query);
                        logger.trace("Invalidated {} cache for query {} due to change in cell {}", name, query, cell);
                    }
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public int estimatedSize() {
        return (int) cache.estimatedSize();
    }

    public int dependencyMappingSize() {
        return dependencyMapping.size();
    }

    private void cleanup(@NotNull SelectQuery query, @NotNull ReadCacheResult res) {
        lock.readLock().lock();
        try {
            for (Cell dependency : res.getDependencies()) {
                dependencyMapping.computeIfPresent(dependency, (k, dependentQueries) -> {
                    dependentQueries.remove(query);
                    return dependentQueries.isEmpty() ? null : dependentQueries;
                });
            }
        } finally {
            lock.readLock().unlock();
        }
    }
}

