package net.staticstudios.data.primative;

import com.google.common.base.Preconditions;

import java.util.function.Consumer;
import java.util.function.Function;

public class PrimitiveBuilder<T> {
    private final Class<T> runtimeType;
    private Function<String, T> decoder;

    public PrimitiveBuilder(Class<T> runtimeType) {
        this.runtimeType = runtimeType;
    }

    public PrimitiveBuilder<T> decoder(Function<String, T> decoder) {
        this.decoder = decoder;
        return this;
    }

    public Primitive<T> build(Consumer<Primitive<T>> consumer) {
        Preconditions.checkNotNull(decoder, "Decoder is null");
        Preconditions.checkNotNull(consumer, "Consumer is null");
        Primitive<T> primitive = new Primitive<>(runtimeType, decoder);
        consumer.accept(primitive);

        return primitive;
    }
}
