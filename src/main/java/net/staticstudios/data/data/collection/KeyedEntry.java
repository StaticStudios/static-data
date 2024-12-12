package net.staticstudios.data.data.collection;

import net.staticstudios.data.PrimaryKey;
import net.staticstudios.data.key.DataKey;

public record KeyedEntry(PrimaryKey pkey, DataKey dataKey, Object value) {
}
