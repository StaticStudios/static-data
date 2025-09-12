package net.staticstudios.data.parse;

import net.staticstudios.data.UniqueData;
import net.staticstudios.data.util.ColumnMetadata;

import java.util.List;

public record UniqueDataMetadata(Class<? extends UniqueData> clazz, String schema, String table,
                                 List<ColumnMetadata> idColumns) {
}
