package net.staticstudios.data;

import net.staticstudios.data.data.Data;
import net.staticstudios.data.data.UniqueData;
import net.staticstudios.data.key.DataKey;

import java.util.Map;
import java.util.Set;

public record DeleteContext(Set<UniqueData> holders, Set<Data<?>> toDelete, Map<DataKey, Object> oldValues) {
}
