package net.staticstudios.data.primative;

import com.google.common.base.Preconditions;
import net.staticstudios.data.util.PostgresUtils;

import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.HashMap;
import java.util.Map;

@SuppressWarnings("unused")
public class Primitives { //todo: test each of these in h2 and pg.
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = new DateTimeFormatterBuilder()
            .append(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
            .appendPattern("xxx")
            .toFormatter()
            .withZone(ZoneId.of("UTC"));

    private static Map<Class<?>, Primitive<?>> primitives;
    public static final Primitive<String> STRING = Primitive.builder(String.class)
            .encoder(s -> s)
            .decoder(s -> s)
            .build(Primitives::register);
    public static final Primitive<Character> CHARACTER = Primitive.builder(Character.class)
            .encoder(c -> Character.toString(c))
            .decoder(s -> s.charAt(0))
            .build(Primitives::register);
    public static final Primitive<Byte> BYTE = Primitive.builder(Byte.class)
            .encoder(b -> Byte.toString(b))
            .decoder(Byte::parseByte)
            .build(Primitives::register);
    public static final Primitive<Short> SHORT = Primitive.builder(Short.class)
            .encoder(s -> Short.toString(s))
            .decoder(Short::parseShort)
            .build(Primitives::register);
    public static final Primitive<Integer> INTEGER = Primitive.builder(Integer.class)
            .encoder(i -> Integer.toString(i))
            .decoder(Integer::parseInt)
            .build(Primitives::register);
    public static final Primitive<Long> LONG = Primitive.builder(Long.class)
            .encoder(l -> Long.toString(l))
            .decoder(Long::parseLong)
            .build(Primitives::register);
    public static final Primitive<Float> FLOAT = Primitive.builder(Float.class)
            .encoder(f -> Float.toString(f))
            .decoder(Float::parseFloat)
            .build(Primitives::register);
    public static final Primitive<Double> DOUBLE = Primitive.builder(Double.class)
            .encoder(d -> Double.toString(d))
            .decoder(Double::parseDouble)
            .build(Primitives::register);
    public static final Primitive<Boolean> BOOLEAN = Primitive.builder(Boolean.class)
            .encoder(b -> Boolean.toString(b))
            .decoder(Boolean::parseBoolean)
            .build(Primitives::register);
    public static final Primitive<java.util.UUID> UUID = Primitive.builder(java.util.UUID.class)
            .encoder(uuid -> uuid == null ? null : uuid.toString())
            .decoder(s -> s == null ? null : java.util.UUID.fromString(s))
            .build(Primitives::register);
    public static final Primitive<Timestamp> TIMESTAMP = Primitive.builder(Timestamp.class)
            .encoder(timestamp -> {
                if (timestamp == null) {
                    return null;
                }

                return TIMESTAMP_FORMATTER.format(timestamp.toInstant());
            })
            .decoder(s -> {
                if (s == null) {
                    return null;
                }

                OffsetDateTime parsedTimestamp = OffsetDateTime.parse(s, TIMESTAMP_FORMATTER);
                return Timestamp.from(parsedTimestamp.toInstant());
            })
            .build(Primitives::register);
    public static final Primitive<byte[]> BYTE_ARRAY = Primitive.builder(byte[].class)
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

    public static Object decodePrimitive(Class<?> type, String value) {
        Primitive<?> primitive = getPrimitive(type);
        Preconditions.checkNotNull(primitive, "No primitive found for type: " + type.getName());
        return primitive.decode(value);
    }

    public static boolean isPrimitive(Class<?> type) {
        return primitives.containsKey(type);
    }

    @SuppressWarnings("unchecked")
    public static <T> T decode(Class<T> type, String value) {
        return (T) getPrimitive(type).decode(value);
    }

    public static String encode(Object value) {
        if (value == null) {
            return null;
        }
        return getPrimitive(value.getClass()).unsafeEncode(value);
    }

    private static void register(Primitive<?> primitive) {
        if (primitives == null) {
            primitives = new HashMap<>();
        }
        primitives.put(primitive.getRuntimeType(), primitive);
    }
}
