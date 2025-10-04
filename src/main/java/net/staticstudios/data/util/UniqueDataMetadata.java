package net.staticstudios.data.util;

import net.staticstudios.data.UniqueData;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

public record UniqueDataMetadata(Class<? extends UniqueData> clazz, String schema, String table,
                                 List<ColumnMetadata> idColumns,
                                 Map<Field, PersistentValueMetadata> persistentValueMetadata,
                                 Map<Field, ReferenceMetadata> referenceMetadata,
                                 Map<Field, PersistentCollectionMetadata> persistentCollectionMetadata) {
}
