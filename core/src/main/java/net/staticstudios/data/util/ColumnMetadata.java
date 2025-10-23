package net.staticstudios.data.util;

import org.jetbrains.annotations.NotNull;

public record ColumnMetadata(String schema, String table, String name, Class<?> type, boolean nullable, boolean indexed,
                             @NotNull String encodedDefaultValue) {
}
