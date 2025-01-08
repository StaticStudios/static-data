package net.staticstudios.data.util;

import net.staticstudios.data.CachedValue;
import net.staticstudios.data.PersistentValue;
import net.staticstudios.data.UniqueData;
import net.staticstudios.data.data.value.InitialCachedValue;
import net.staticstudios.data.data.value.InitialPersistentValue;

import java.util.Map;

public record InsertContext(
        UniqueData holder,
        Map<PersistentValue<?>, InitialPersistentValue> initialPersistentValues,
        Map<CachedValue<?>, InitialCachedValue> initialCachedValues
) {
}
