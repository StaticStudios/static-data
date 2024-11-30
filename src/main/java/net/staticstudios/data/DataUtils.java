package net.staticstudios.data;

public class DataUtils {
    public static Object getDefaultValue(Class<?> clazz) {
        if (clazz == Integer.class) {
            return 0;
        } else if (clazz == Long.class) {
            return 0L;
        } else if (clazz == Double.class) {
            return 0.0;
        } else if (clazz == Float.class) {
            return 0.0f;
        } else if (clazz == Boolean.class) {
            return false;
        } else {
            return null;
        }
    }

    public static Object getValue(Class<?> clazz, Object value) {
        if (value == null) {
            return getDefaultValue(clazz);
        }
        if (clazz == Integer.class) {
            return ((Number) value).intValue();
        }
        if (clazz == Long.class) {
            return ((Number) value).longValue();
        }
        if (clazz == Double.class) {
            return ((Number) value).doubleValue();
        }
        if (clazz == Float.class) {
            return ((Number) value).floatValue();
        }
        return value;
    }
}
