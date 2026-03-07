package net.staticstudios.data.util;

import org.jetbrains.annotations.Nullable;

import java.util.Set;

public class ReadCacheResult {
    private final ColumnValuePairs columnValuePairs;
    private final Set<Cell> dependencies;

    public ReadCacheResult(ColumnValuePairs columnValuePairs, Set<Cell> dependencies) {
        this.columnValuePairs = columnValuePairs;
        this.dependencies = dependencies;
    }

    public @Nullable Object getValue(String column) {
        for (ColumnValuePair pair : columnValuePairs) {
            if (pair.column().equals(column)) {
                return pair.value();
            }
        }
        return null;
    }

    public Set<Cell> getDependencies() {
        return dependencies;
    }

    @Override
    public String toString() {
        return "ReadCacheResult[" +
                "columnValuePairs=" + columnValuePairs + ", " +
                "dependencies=" + dependencies + ']';
    }
}
