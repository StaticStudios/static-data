package net.staticstudios.data.primative;

import com.google.common.base.Preconditions;
import org.jetbrains.annotations.NotNull;

import java.util.function.Consumer;
import java.util.function.Function;

public class PrimitiveBuilder<T> {
    private final Class<T> runtimeType;
    private Function<String, T> decoder;
    private Function<T, String> encoder;
    private String h2SQLType;
    private String pgSQLType;

    public PrimitiveBuilder(Class<T> runtimeType) {
        this.runtimeType = runtimeType;
    }

    public PrimitiveBuilder<T> decoder(Function<@NotNull String, @NotNull T> decoder) {
        this.decoder = decoder;
        return this;
    }

    /**
     * Note that the encoder should encode the value to a string the exact same as Postgres would.
     *
     * @param encoder The encoder function
     * @return The builder
     */
    public PrimitiveBuilder<T> encoder(Function<@NotNull T, @NotNull String> encoder) {
        this.encoder = encoder;
        return this;
    }

    public PrimitiveBuilder<T> h2SQLType(String h2SQLType) {
        this.h2SQLType = h2SQLType;
        return this;
    }

    public PrimitiveBuilder<T> pgSQLType(String pgSQLType) {
        this.pgSQLType = pgSQLType;
        return this;
    }


    public Primitive<T> build(Consumer<Primitive<T>> consumer) {
        Preconditions.checkNotNull(decoder, "Decoder is null");
        Preconditions.checkNotNull(encoder, "Encoder is null");
        Preconditions.checkNotNull(consumer, "Consumer is null");
        Preconditions.checkNotNull(h2SQLType, "H2 SQL Type is null");
        Preconditions.checkNotNull(pgSQLType, "Postgres SQL Type is null");

        Primitive<T> primitive = new Primitive<>(runtimeType, decoder, encoder, h2SQLType, pgSQLType);
        consumer.accept(primitive);

        return primitive;
    }
}
