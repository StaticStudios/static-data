package net.staticstudios.data.util;

import net.staticstudios.data.primative.Primitives;

public class SQLUtils {
    public static String getH2SqlType(Class<?> clazz) {
        if (Primitives.isPrimitive(clazz)) {
            return Primitives.getPrimitive(clazz).getH2SQLType();
        }
        throw new IllegalArgumentException("Unsupported class type: " + clazz.getName());
    }

    public static String getPgSqlType(Class<?> clazz) {
        if (Primitives.isPrimitive(clazz)) {
            return Primitives.getPrimitive(clazz).getPgSQLType();
        }
        throw new IllegalArgumentException("Unsupported class type: " + clazz.getName());
    }

    public static String parseDefaultValue(Class<?> clazz, String defaultValue) {
        if (clazz == String.class) {
            return "'" + defaultValue.replace("'", "''") + "'";
        }
        if (clazz == Boolean.class) {
            return Boolean.parseBoolean(defaultValue) ? "TRUE" : "FALSE";
        }
        return defaultValue;
    }
}
