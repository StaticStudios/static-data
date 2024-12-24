package net.staticstudios.data.key;

import java.util.Arrays;

public class DataKey {
    private final Object[] parts;

    public DataKey(Object... parts) {
        this.parts = parts;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        DataKey dataKey = (DataKey) obj;
        return Arrays.equals(parts, dataKey.parts);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(parts) + getClass().hashCode();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" +
                "parts=" + Arrays.toString(parts) +
                '}';
    }
}
