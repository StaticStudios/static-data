package net.staticstudios.data.impl.redis;

import java.time.Instant;

public record RedisCacheEntry(String value, Instant entryInstant) {
}
