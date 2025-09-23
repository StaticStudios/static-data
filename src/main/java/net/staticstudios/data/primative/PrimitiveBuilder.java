package net.staticstudios.data.primative;

import com.google.common.base.Preconditions;

import java.util.function.Consumer;
import java.util.function.Function;

public class PrimitiveBuilder<T> {
    private final Class<T> runtimeType;
    private Function<String, T> decoder;
    private Function<T, String> encoder;

    public PrimitiveBuilder(Class<T> runtimeType) {
        this.runtimeType = runtimeType;
    }

    public PrimitiveBuilder<T> decoder(Function<String, T> decoder) {
        this.decoder = decoder;
        return this;
    }

    /**
     * Note that the encoder should encode the value to a string the exact same as Postgres would.
     *
     * @param encoder The encoder function
     * @return The builder
     */
    public PrimitiveBuilder<T> encoder(Function<T, String> encoder) {
        this.encoder = encoder;
        return this;
    }

    public Primitive<T> build(Consumer<Primitive<T>> consumer) {
        Preconditions.checkNotNull(decoder, "Decoder is null");
        Preconditions.checkNotNull(encoder, "Encoder is null");
        Preconditions.checkNotNull(consumer, "Consumer is null");

        Primitive<T> primitive = new Primitive<>(runtimeType, decoder, encoder);
        consumer.accept(primitive);

        return primitive;
    }
}
