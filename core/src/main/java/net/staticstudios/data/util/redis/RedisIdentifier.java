package net.staticstudios.data.util.redis;

import net.staticstudios.data.util.ColumnValuePairs;
import org.jspecify.annotations.NonNull;

public record RedisIdentifier(String holderSchema, String holderTable, String identifier, ColumnValuePairs idColumns) {
    @Override
    public @NonNull String toString() {
        return RedisUtils.toKey(this);
    }
}
