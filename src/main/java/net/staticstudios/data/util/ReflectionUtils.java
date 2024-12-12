package net.staticstudios.data.util;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

public class ReflectionUtils {

    /**
     * Get all fields from this class AND its superclasses
     *
     * @param clazz The class to get fields from
     * @return A list of fields
     */
    public static List<Field> getFields(Class<?> clazz) {
        List<Field> fields = new ArrayList<>(List.of(clazz.getDeclaredFields()));

        if (clazz.getSuperclass() != null) {
            fields.addAll(getFields(clazz.getSuperclass()));
        }

        return fields;
    }
}
