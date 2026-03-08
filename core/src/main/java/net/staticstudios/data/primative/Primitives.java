package net.staticstudios.data.primative;

import com.google.common.base.Preconditions;
import org.jetbrains.annotations.Nullable;

import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.HashMap;
import java.util.Map;

@SuppressWarnings("unused")
public class Primitives {
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = new DateTimeFormatterBuilder()
            .append(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
            .appendPattern("xxx")
            .toFormatter()
            .withZone(ZoneId.of("UTC"));

    private static Map<Class<?>, Primitive<?>> primitives;
    public static final Primitive<String> STRING = Primitive.builder(String.class)
            .h2SQLType("TEXT")
            .pgSQLType("TEXT")
            .encoder(s -> s)
            .decoder(s -> s)
            .copier(s -> s)
            .build(Primitives::register);
    public static final Primitive<Integer> INTEGER = Primitive.builder(Integer.class)
            .h2SQLType("INTEGER")
            .pgSQLType("INTEGER")
            .encoder(i -> Integer.toString(i))
            .copier(i -> i)
            .decoder(Integer::parseInt)
            .build(Primitives::register);
    public static final Primitive<Long> LONG = Primitive.builder(Long.class)
            .h2SQLType("BIGINT")
            .pgSQLType("BIGINT")
            .encoder(l -> Long.toString(l))
            .decoder(Long::parseLong)
            .copier(l -> l)
            .build(Primitives::register);
    public static final Primitive<Float> FLOAT = Primitive.builder(Float.class)
            .h2SQLType("REAL")
            .pgSQLType("REAL")
            .encoder(f -> Float.toString(f))
            .decoder(Float::parseFloat)
            .copier(f -> f)
            .build(Primitives::register);
    public static final Primitive<Double> DOUBLE = Primitive.builder(Double.class)
            .h2SQLType("DOUBLE PRECISION")
            .pgSQLType("DOUBLE PRECISION")
            .encoder(d -> Double.toString(d))
            .decoder(Double::parseDouble)
            .copier(d -> d)
            .build(Primitives::register);
    public static final Primitive<Boolean> BOOLEAN = Primitive.builder(Boolean.class)
            .h2SQLType("BOOLEAN")
            .pgSQLType("BOOLEAN")
            .encoder(b -> Boolean.toString(b))
            .decoder(Boolean::parseBoolean)
            .copier(b -> b)
            .build(Primitives::register);
    public static final Primitive<java.util.UUID> UUID = Primitive.builder(java.util.UUID.class)
            .h2SQLType("UUID")
            .pgSQLType("UUID")
            .encoder(java.util.UUID::toString)
            .decoder(java.util.UUID::fromString)
            .copier(uuid -> uuid)
            .build(Primitives::register);
    public static final Primitive<Timestamp> TIMESTAMP = Primitive.builder(Timestamp.class)
            .h2SQLType("TIMESTAMP WITH TIME ZONE")
            .pgSQLType("TIMESTAMPTZ")
            .encoder(timestamp -> TIMESTAMP_FORMATTER.format(timestamp.toInstant()))
            .decoder(s -> Timestamp.from(OffsetDateTime.parse(s, TIMESTAMP_FORMATTER).toInstant()))
            .copier(timestamp -> new Timestamp(timestamp.getTime()))
            .build(Primitives::register);

    // dropping support for byte[] for the time being, im running into weird issues on the h2 side. also the javac stuff is having issues parsing ...<byte[]> types.
//    public static final Primitive<byte[]> BYTE_ARRAY = Primitive.builder(byte[].class)
//            .h2SQLType("BINARY LARGE OBJECT")
//            .pgSQLType("BYTEA")
//            .encoder(PostgresUtils::toHex)
//            .decoder(PostgresUtils::toBytes)
//            .copier(bytes -> {
//                byte[] copy = new byte[bytes.length];
//                System.arraycopy(bytes, 0, copy, 0, bytes.length);
//                return copy;
//            })
//            .build(Primitives::register);

    @SuppressWarnings("unchecked")
    public static <T> Primitive<T> getPrimitive(Class<T> type) {
        return (Primitive<T>) primitives.get(type);
    }

    public static Object decodePrimitive(Class<?> type, String value) {
        Primitive<?> primitive = getPrimitive(type);
        Preconditions.checkNotNull(primitive, "No primitive found for type: " + type.getName());
        return primitive.decode(value);
    }

    public static boolean isPrimitive(Class<?> type) {
        return primitives.containsKey(type);
    }

    public static <T> T decode(Class<T> type, String value) {
        return getPrimitive(type).decode(value);
    }

    public static String encode(@Nullable Object value) {
        if (value == null) {
            return null;
        }
        return encode(value, value.getClass());
    }

    public static <T> T copy(T value, Class<T> type) {
        return getPrimitive(type).copy(value);
    }

    private static <T> String encode(Object value, Class<T> type) {
        return getPrimitive(type).encode(type.cast(value));
    }

    private static void register(Primitive<?> primitive) {
        if (primitives == null) {
            primitives = new HashMap<>();
        }
        primitives.put(primitive.getRuntimeType(), primitive);
    }
}
