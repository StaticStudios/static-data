package net.staticstudios.data.primative;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.function.Function;

public class Primitive<T> {
    private final Class<T> runtimeType;
    private final Function<@NotNull String, @NotNull T> decoder;
    private final Function<@NotNull T, @NotNull String> encoder;
    private final String h2SQLType;
    private final String pgSQLType;

    public Primitive(Class<T> runtimeType, Function<@NotNull String, @NotNull T> decoder, Function<@NotNull T, @NotNull String> encoder, String h2SQLType, String pgSQLType) {
        this.runtimeType = runtimeType;
        this.decoder = decoder;
        this.encoder = encoder;
        this.h2SQLType = h2SQLType;
        this.pgSQLType = pgSQLType;
    }

    public static <T> PrimitiveBuilder<T> builder(Class<T> runtimeType) {
        return new PrimitiveBuilder<>(runtimeType);
    }

    public @Nullable T decode(@Nullable String value) {
        if (value == null) {
            return null;
        }
        return decoder.apply(value);
    }

    public @Nullable String encode(@Nullable T value) {
        if (value == null) {
            return null;
        }
        return encoder.apply(value);
    }

    public String getH2SQLType() {
        return h2SQLType;
    }

    public String getPgSQLType() {
        return pgSQLType;
    }

    public Class<T> getRuntimeType() {
        return runtimeType;
    }
}
