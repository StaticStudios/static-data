package net.staticstudios.data.primative;

import java.util.function.Function;

public class Primitive<T> {
    private final Class<T> runtimeType;
    private final Function<String, T> decoder;
    private final Function<T, String> encoder;
    private final boolean nullable;
    private final T defaultValue;

    public Primitive(Class<T> runtimeType, Function<String, T> decoder, Function<T, String> encoder, boolean nullable, T defaultValue) {
        this.runtimeType = runtimeType;
        this.decoder = decoder;
        this.encoder = encoder;
        this.nullable = nullable;
        this.defaultValue = defaultValue;
    }

    public static <T> PrimitiveBuilder<T> builder(Class<T> runtimeType) {
        return new PrimitiveBuilder<>(runtimeType);
    }

    public T decode(String value) {
        return decoder.apply(value);
    }

    public String encode(T value) {
        return encoder.apply(value);
    }

    public String unsafeEncode(Object value) {
        return encoder.apply(runtimeType.cast(value));
    }

    public boolean isNullable() {
        return nullable;
    }

    public T getDefaultValue() {
        return defaultValue;
    }

    public Class<T> getRuntimeType() {
        return runtimeType;
    }
}
