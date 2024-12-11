package net.staticstudios.data.v2;

import java.util.Arrays;

public class DataKey {
    private final Object[] parts;

    private DataKey(Object... parts) {
        this.parts = parts;
    }

    public static DataKey of(Object... parts) {
        return new DataKey(parts);
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
        return Arrays.hashCode(parts);
    }

    @Override
    public String toString() {
        return "DataKey{" +
                "parts=" + Arrays.toString(parts) +
                '}';
    }
}
