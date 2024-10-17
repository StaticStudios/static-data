package net.staticstudios.data;

import org.jetbrains.annotations.Nullable;
import org.postgresql.jdbc.PgArray;

import java.sql.Timestamp;
import java.util.Base64;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

public enum DatabaseSupportedType {
    STRING(String.class, str -> str, str -> str),
    CHARACTER(Character.class, obj -> {
        if (obj instanceof String str) {
            return str.charAt(0);
        }
        return null;
    }, String::valueOf, str -> str.charAt(0)),
    SHORT(Short.class, obj -> {
        if (obj instanceof Number num) {
            return num.shortValue();
        }
        return null;
    }, String::valueOf, Short::parseShort),
    INTEGER(Integer.class, obj -> {
        if (obj instanceof Number num) {
            return num.intValue();
        }
        return null;
    }, String::valueOf, Integer::parseInt),
    LONG(Long.class, obj -> {
        if (obj instanceof Number num) {
            return num.longValue();
        }
        return null;
    }, String::valueOf, Long::parseLong),
    FLOAT(Float.class, obj -> {
        if (obj instanceof Number num) {
            return num.floatValue();
        }
        return null;
    }, String::valueOf, Float::parseFloat),
    DOUBLE(Double.class, obj -> {
        if (obj instanceof Number num) {
            return num.doubleValue();
        }
        return null;
    }, String::valueOf, Double::parseDouble),
    BOOLEAN(Boolean.class, String::valueOf, Boolean::parseBoolean),
    UUID(java.util.UUID.class, java.util.UUID::toString, java.util.UUID::fromString),
    TIMESTAMP(Timestamp.class, Timestamp::toString, Timestamp::valueOf),
    BYTE_ARRAY(byte[].class, bytes -> {
        Base64.Encoder encoder = Base64.getEncoder();
        return encoder.encodeToString(bytes);
    }, str -> {
        Base64.Decoder decoder = Base64.getDecoder();
        return decoder.decode(str);
    }),
    UUID_ARRAY(java.util.UUID[].class, obj -> {
        if (obj instanceof PgArray pgArray) {
            try {
                return (java.util.UUID[]) pgArray.getArray();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return null;
    }, obj -> {
        if (obj instanceof java.util.UUID[] uuids) {
            StringBuilder builder = new StringBuilder();
            for (java.util.UUID uuid : uuids) {
                builder.append(uuid.toString()).append(",");
            }
            return builder.toString();
        }
        return null;
    }, str -> {
        String[] parts = str.split(",");
        java.util.UUID[] uuids = new java.util.UUID[parts.length];
        for (int i = 0; i < parts.length; i++) {
            uuids[i] = java.util.UUID.fromString(parts[i]);
        }
        return uuids;
    });

    private static Set<Class<?>> SUPPORTED_TYPES;
    private final Class<?> type;
    private final Function<Object, String> encoder;
    private final Function<String, Object> decoder;
    private final @Nullable Function<Object, Object> deserializer;

    <T> DatabaseSupportedType(Class<T> type, Function<T, String> encoder, Function<String, T> decoder) {
        this(type, null, encoder, decoder);
    }

    <T> DatabaseSupportedType(Class<T> type, @Nullable Function<Object, T> deserializer, Function<T, String> encoder, Function<String, T> decoder) {
        this.type = type;
        this.deserializer = (Function<Object, Object>) deserializer;
        this.encoder = (Function<Object, String>) encoder;
        this.decoder = (Function<String, Object>) decoder;
    }

    private static Set<Class<?>> getSupportedTypes() {
        if (SUPPORTED_TYPES == null) {
            SUPPORTED_TYPES = new HashSet<>();

            for (DatabaseSupportedType type : values()) {
                SUPPORTED_TYPES.add(type.type);
            }
        }

        return SUPPORTED_TYPES;
    }

    public static boolean isSupported(Class<?> type) {
        return getSupportedTypes().contains(type);
    }

    /**
     * Attempts to deserialize the given value into a supported type.
     *
     * @param value      The value to deserialize.
     * @param targetType The type to deserialize the value into.
     * @return The deserialized value, or null if it could not be deserialized.
     */
    public static Object deserialize(Object value, Class<?> targetType) {
        if (value == null) {
            return null;
        }

        for (DatabaseSupportedType supportedType : values()) {
            if (supportedType.type.isAssignableFrom(targetType)) {
                if (supportedType.deserializer == null) {
                    return null;
                }

                return supportedType.deserializer.apply(value);
            }
        }

        return null;
    }

    public static Object decode(String encoded) {
        String[] parts = encoded.split(",", 2);
        String type = parts[0];
        String value = parts[1];

        for (DatabaseSupportedType supportedType : values()) {
            if (supportedType.name().equals(type)) {
                return supportedType.decoder.apply(value);
            }
        }

        throw new IllegalArgumentException("Unsupported type: " + type);
    }


    public static String encode(Object value) {
        DatabaseSupportedType supportedType = null;

        for (DatabaseSupportedType typeEnum : values()) {
            if (typeEnum.getType().isInstance(value)) {
                supportedType = typeEnum;
                break;
            }
        }

        if (supportedType == null) {
            throw new IllegalArgumentException("Unsupported type: " + value.getClass().getName());
        }

        return supportedType.name() + "," + supportedType.encoder.apply(value);
    }

    public Class<?> getType() {
        return type;
    }
}
