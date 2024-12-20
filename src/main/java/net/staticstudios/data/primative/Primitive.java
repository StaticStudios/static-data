package net.staticstudios.data.primative;

import java.util.function.Function;

public class Primitive<T> {
    private final Class<T> runtimeType;
    private final Function<String, T> decoder;

    public Primitive(Class<T> runtimeType, Function<String, T> decoder) {
        this.runtimeType = runtimeType;
        this.decoder = decoder;
    }

    public static <T> PrimitiveBuilder<T> builder(Class<T> runtimeType) {
        return new PrimitiveBuilder<>(runtimeType);
    }

    public T decode(String value) {
        return decoder.apply(value);
    }

    public Class<T> getRuntimeType() {
        return runtimeType;
    }
}
