package net.staticstudios.data;

import java.time.Instant;

public record CacheEntry(Object value, Instant instant) {

    public static <T> CacheEntry of(T value) {
        return new CacheEntry(value, Instant.now());
    }

    public static <T> CacheEntry of(T value, Instant instant) {
        return new CacheEntry(value, instant);
    }
}
