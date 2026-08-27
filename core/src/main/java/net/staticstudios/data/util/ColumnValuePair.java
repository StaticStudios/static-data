package net.staticstudios.data.util;

import java.util.Objects;

public final class ColumnValuePair {
    private final String column;
    private final Object value;
    private final int hashCode;

    public ColumnValuePair(String column, Object value) {
        this.column = column;
        this.value = value;
        this.hashCode = 31 * Objects.hashCode(column) + Objects.hashCode(value);
    }

    public static ColumnValuePair of(String column, Object value) {
        return new ColumnValuePair(column, value);
    }

    public String column() {
        return column;
    }

    public Object value() {
        return value;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (ColumnValuePair) obj;
        return Objects.equals(this.column, that.column) &&
                Objects.equals(this.value, that.value);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public String toString() {
        return "ColumnValuePair[" +
                "name=" + column + ", " +
                "value=" + value + ']';
    }

}
