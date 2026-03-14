package net.staticstudios.data.util;

import org.jetbrains.annotations.NotNull;

public record EnqueuedDatabaseTaskKey(@NotNull ColumnValuePairs holderPairs, SQLTransaction.Statement[] statements) {
}
