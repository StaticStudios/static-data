package net.staticstudios.data.data.collection;

import net.staticstudios.data.DataKey;
import net.staticstudios.data.PrimaryKey;

public record KeyedEntry(PrimaryKey pkey, DataKey dataKey, Object value) {
}
