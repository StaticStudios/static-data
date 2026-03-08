package net.staticstudios.data.util;

import java.util.Set;

public class ReadCacheResult {
    private final Object value;
    private final Set<Cell> dependencies;

    public ReadCacheResult(Object value, Set<Cell> dependencies) {
        this.value = value;
        this.dependencies = dependencies;
    }

    public Object getValue() {
        return value;
    }

    public Set<Cell> getDependencies() {
        return dependencies;
    }

    @Override
    public String toString() {
        return "ReadCacheResult[" +
                "value=" + value +
                ", dependencies=" + dependencies + ']';
    }
}
