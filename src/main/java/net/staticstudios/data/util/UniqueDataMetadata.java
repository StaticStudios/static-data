package net.staticstudios.data.util;

import net.staticstudios.data.UniqueData;

import java.util.List;

public record UniqueDataMetadata(Class<? extends UniqueData> clazz, String schema, String table,
                                 List<ColumnMetadata> idColumns) {
}
