package net.staticstudios.data.primative;

import net.staticstudios.data.util.PostgresUtils;

import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.HashMap;
import java.util.Map;

@SuppressWarnings("unused")
public class Primitives {
    // General rule of thumb: if the Primitive is a Java primitive, it should not be nullable, everything else should be nullable.


    private static Map<Class<?>, Primitive<?>> primitives;
    public static final Primitive<String> STRING = Primitive.builder(String.class)
            .nullable(true)
            .encoder(s -> s)
            .decoder(s -> s)
            .build(Primitives::register);
    public static final Primitive<Character> CHARACTER = Primitive.builder(Character.class)
            .nullable(false)
            .encoder(c -> Character.toString(c))
            .decoder(s -> s.charAt(0))
            .build(Primitives::register);
    public static final Primitive<Byte> BYTE = Primitive.builder(Byte.class)
            .nullable(false)
            .encoder(b -> Byte.toString(b))
            .decoder(Byte::parseByte)
            .build(Primitives::register);
    public static final Primitive<Short> SHORT = Primitive.builder(Short.class)
            .nullable(false)
            .encoder(s -> Short.toString(s))
            .decoder(Short::parseShort)
            .build(Primitives::register);
    public static final Primitive<Integer> INTEGER = Primitive.builder(Integer.class)
            .nullable(false)
            .encoder(i -> Integer.toString(i))
            .decoder(Integer::parseInt)
            .build(Primitives::register);
    public static final Primitive<Long> LONG = Primitive.builder(Long.class)
            .nullable(false)
            .encoder(l -> Long.toString(l))
            .decoder(Long::parseLong)
            .build(Primitives::register);
    public static final Primitive<Float> FLOAT = Primitive.builder(Float.class)
            .nullable(false)
            .encoder(f -> Float.toString(f))
            .decoder(Float::parseFloat)
            .build(Primitives::register);
    public static final Primitive<Double> DOUBLE = Primitive.builder(Double.class)
            .nullable(false)
            .encoder(d -> Double.toString(d))
            .decoder(Double::parseDouble)
            .build(Primitives::register);
    public static final Primitive<Boolean> BOOLEAN = Primitive.builder(Boolean.class)
            .nullable(false)
            .encoder(b -> Boolean.toString(b))
            .decoder(Boolean::parseBoolean)
            .build(Primitives::register);
    public static final Primitive<java.util.UUID> UUID = Primitive.builder(java.util.UUID.class)
            .nullable(true)
            .encoder(uuid -> uuid == null ? null : uuid.toString())
            .decoder(s -> s == null ? null : java.util.UUID.fromString(s))
            .build(Primitives::register);
    public static final Primitive<Timestamp> TIMESTAMP = Primitive.builder(Timestamp.class)
            .nullable(true)
            .encoder(timestamp -> {
                if (timestamp == null) {
                    return null;
                }

                return new DateTimeFormatterBuilder()
                        .append(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
                        .appendPattern("xxx")
                        .toFormatter()
                        .withZone(ZoneId.of("UTC"))
                        .format(timestamp.toInstant());
            })
            .decoder(s -> {
                if (s == null) {
                    return null;
                }
                System.out.println("parsing timestamp: " + s);

                OffsetDateTime parsedTimestamp = OffsetDateTime.parse(s, DateTimeFormatter.ISO_OFFSET_DATE_TIME);
                return Timestamp.from(parsedTimestamp.toInstant());
            })
            .build(Primitives::register);
    public static final Primitive<byte[]> BYTE_ARRAY = Primitive.builder(byte[].class)
            .nullable(true)
            .encoder(b -> {
                if (b == null) {
                    return null;
                }

                return PostgresUtils.toHex(b);
            })
            .decoder(s -> {
                if (s == null) {
                    return null;
                }

                return PostgresUtils.toBytes(s);
            })
            .build(Primitives::register);

    public static Primitive<?> getPrimitive(Class<?> type) {
        return primitives.get(type);
    }

    public static boolean isPrimitive(Class<?> type) {
        return primitives.containsKey(type);
    }

    @SuppressWarnings("unchecked")
    public static <T> T decode(Class<T> type, String value) {
        return (T) getPrimitive(type).decode(value);
    }

    private static void register(Primitive<?> primitive) {
        if (primitives == null) {
            primitives = new HashMap<>();
        }
        primitives.put(primitive.getRuntimeType(), primitive);
    }
}
