package net.staticstudios.data.util;

public enum OnDelete {
    CASCADE("CASCADE"),
    SET_NULL("SET NULL"),
    NO_ACTION("NO ACTION");

    private final String sql;

    OnDelete(String sql) {
        this.sql = sql;
    }

    @Override
    public String toString() {
        return sql;
    }
}
