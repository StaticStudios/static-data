package net.staticstudios.data.util;

import net.staticstudios.data.UniqueData;

public record CachedValueMetadata(Class<? extends UniqueData> holderClass, String holderSchema, String holderTable,
                                  String identifier, int expireAfterSeconds) {
}
