package net.staticstudios.data.util;

import org.jetbrains.annotations.Nullable;

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

    /**
     * Get all fields from this class AND its superclasses of a specific type
     *
     * @param clazz     The class to get fields from
     * @param fieldType The type of fields to get
     * @return A list of fields
     */
    public static List<Field> getFields(Class<?> clazz, Class<?> fieldType) {
        List<Field> fields = getFields(clazz);
        fields.removeIf(field -> !fieldType.isAssignableFrom(field.getType()));
        return fields;
    }

    public static <T> List<FieldInstancePair<T>> getFieldInstancePairs(Object instance, Class<T> fieldType) {
        List<Field> fields = getFields(instance.getClass(), fieldType);
        List<FieldInstancePair<T>> instances = new ArrayList<>();
        for (Field field : fields) {
            field.setAccessible(true);
            try {
                T value = fieldType.cast(field.get(instance));
                if (value != null) {
                    instances.add(new FieldInstancePair<>(field, value));
                }
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
        return instances;
    }

    public static <T> List<T> getFieldInstances(Object instance, Class<T> fieldType) {
        List<FieldInstancePair<T>> pairs = getFieldInstancePairs(instance, fieldType);
        List<T> instances = new ArrayList<>();
        for (FieldInstancePair<T> pair : pairs) {
            instances.add(pair.instance());
        }
        return instances;
    }


    public static @Nullable Class<?> getGenericType(Field field) {
        if (field.getGenericType() instanceof Class<?>) {
            return (Class<?>) field.getGenericType();
        } else if (field.getGenericType() instanceof java.lang.reflect.ParameterizedType parameterizedType) {
            return (Class<?>) parameterizedType.getRawType();
        }
        return null;
    }
}
