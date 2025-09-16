package net.staticstudios.data.primative;

import com.google.common.base.Preconditions;

import java.util.function.Consumer;
import java.util.function.Function;

public class PrimitiveBuilder<T> {
    private final Class<T> runtimeType;
    private Function<String, T> decoder;
    private Function<T, String> encoder;
    private Boolean nullable;
    private T defaultValue;

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

    public PrimitiveBuilder<T> nullable(boolean nullable) {
        this.nullable = nullable;
        return this;
    }

    public PrimitiveBuilder<T> defaultValue(T defaultValue) {
        this.defaultValue = defaultValue;
        return this;
    }

    public Primitive<T> build(Consumer<Primitive<T>> consumer) {
        Preconditions.checkNotNull(decoder, "Decoder is null");
        Preconditions.checkNotNull(encoder, "Encoder is null");
        Preconditions.checkNotNull(consumer, "Consumer is null");
        Preconditions.checkNotNull(nullable, "Nullable flag is null");

        if (!nullable) {
            Preconditions.checkNotNull(defaultValue, "Default value is null");
        }

        Primitive<T> primitive = new Primitive<>(runtimeType, decoder, encoder, nullable, defaultValue);
        consumer.accept(primitive);

        return primitive;
    }
}
