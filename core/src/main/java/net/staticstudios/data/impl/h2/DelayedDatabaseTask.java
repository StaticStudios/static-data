package net.staticstudios.data.impl.h2;

public record DelayedDatabaseTask(String sql, Object[] params) {
}
