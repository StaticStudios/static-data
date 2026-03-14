package net.staticstudios.data.util;

import org.jetbrains.annotations.NotNull;

import java.util.List;

public record EnqueuedDatabaseTaskKey(@NotNull ColumnValuePairs holderPairs,
                                      List<SQLTransaction.Statement> sqlStatements) {
}
