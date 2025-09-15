package net.staticstudios.data.util;

public class SQLUtils {
    public static String getSqlType(Class<?> clazz) {
        if (clazz.equals(String.class)) {
            return "TEXT";
        }
        if (clazz.equals(Integer.class) || clazz.equals(int.class)) {
            return "INTEGER";
        }
        if (clazz.equals(Long.class) || clazz.equals(long.class)) {
            return "BIGINT";
        }
        if (clazz.equals(Boolean.class) || clazz.equals(boolean.class)) {
            return "BOOLEAN";
        }
        if (clazz.equals(Double.class) || clazz.equals(double.class)) {
            return "DOUBLE PRECISION";
        }
        if (clazz.equals(Float.class) || clazz.equals(float.class)) {
            return "REAL";
        }
        if (clazz.equals(java.util.UUID.class)) {
            return "UUID";
        }
        throw new IllegalArgumentException("Unsupported class type: " + clazz.getName());
    }

    public static String parseDefaultValue(Class<?> clazz, String defaultValue) {
        try {
            return switch (getSqlType(clazz)) {
                case "TEXT" -> "'" + defaultValue.replace("'", "''") + "'";
                case "BOOLEAN" -> Boolean.parseBoolean(defaultValue) ? "TRUE" : "FALSE";
                default -> defaultValue;
            };
        } catch (IllegalArgumentException e) {
            return defaultValue;
        }
    }
}
