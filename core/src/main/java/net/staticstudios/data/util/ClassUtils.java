package net.staticstudios.data.util;

import net.staticstudios.data.DataManager;

public class ClassUtils {

    @SuppressWarnings("unchecked")
    public static <T> Class<T> forName(String className) {
        try {
            return (Class<T>) Class.forName(className, false, DataManager.class.getClassLoader());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Class not found: " + className, e);
        }
    }
}
