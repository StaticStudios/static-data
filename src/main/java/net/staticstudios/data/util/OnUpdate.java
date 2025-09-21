package net.staticstudios.data.util;

public enum OnUpdate {
    CASCADE("CASCADE"),
    NO_ACTION("NO ACTION");

    private final String sql;

    OnUpdate(String sql) {
        this.sql = sql;
    }

    @Override
    public String toString() {
        return sql;
    }
}
