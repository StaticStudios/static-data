package net.staticstudios.data;

import net.staticstudios.data.data.UniqueData;
import net.staticstudios.data.data.value.persistent.InitialPersistentValue;
import net.staticstudios.data.data.value.persistent.PersistentValue;
import net.staticstudios.data.data.value.redis.CachedValue;
import net.staticstudios.data.data.value.redis.InitialCachedValue;

import java.util.Map;

public record InsertContext(
        UniqueData holder,
        Map<PersistentValue<?>, InitialPersistentValue> initialPersistentValues,
        Map<CachedValue<?>, InitialCachedValue> initialCachedValues
) {
}
