package net.staticstudios.data.primative;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

//todo: these need an encoder for use in redis
public class Primitives {
    private static Map<Class<?>, Primitive<?>> primitives;
    public static final Primitive<String> STRING = Primitive.builder(String.class)
            .decoder(s -> s)
            .build(Primitives::register);
    public static final Primitive<Character> CHARACTER = Primitive.builder(Character.class)
            .decoder(s -> s.charAt(0))
            .build(Primitives::register);
    public static final Primitive<Byte> BYTE = Primitive.builder(Byte.class)
            .decoder(Byte::parseByte)
            .build(Primitives::register);
    public static final Primitive<Short> SHORT = Primitive.builder(Short.class)
            .decoder(Short::parseShort)
            .build(Primitives::register);
    public static final Primitive<Integer> INTEGER = Primitive.builder(Integer.class)
            .decoder(Integer::parseInt)
            .build(Primitives::register);
    public static final Primitive<Long> LONG = Primitive.builder(Long.class)
            .decoder(Long::parseLong)
            .build(Primitives::register);
    public static final Primitive<Float> FLOAT = Primitive.builder(Float.class)
            .decoder(Float::parseFloat)
            .build(Primitives::register);
    public static final Primitive<Double> DOUBLE = Primitive.builder(Double.class)
            .decoder(Double::parseDouble)
            .build(Primitives::register);
    public static final Primitive<Boolean> BOOLEAN = Primitive.builder(Boolean.class)
            .decoder(Boolean::parseBoolean)
            .build(Primitives::register);
    public static final Primitive<java.util.UUID> UUID = Primitive.builder(java.util.UUID.class)
            .decoder(java.util.UUID::fromString)
            .build(Primitives::register);
    public static final Primitive<Timestamp> TIMESTAMP = Primitive.builder(Timestamp.class)
            .decoder(Timestamp::valueOf) //todo:validate this decoder works
            .build(Primitives::register);
    public static final Primitive<byte[]> BYTE_ARRAY = Primitive.builder(byte[].class)
            .decoder(s -> {
                //todo: byte arrays come from postgres in a specific format, decode them accordingly
                throw new UnsupportedOperationException("Not implemented");
            })
            .build(Primitives::register);

    public static Primitive<?> getPrimitive(Class<?> type) {
        return primitives.get(type);
    }

    public static boolean isPrimitive(Class<?> type) {
        return primitives.containsKey(type);
    }

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
