package net.staticstudios.data.compiler.javac;

import net.staticstudios.data.utils.Constants;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class ParsedColumnAnnotation extends ParsedAnnotation {
    private final @NotNull String name;
    private final @Nullable String schema;
    private final @Nullable String table;
    private final boolean index;
    private final boolean nullable;
    private final boolean unique;

    public ParsedColumnAnnotation(
            @NotNull String name,
            @Nullable String schema,
            @Nullable String table,
            boolean index,
            boolean nullable,
            boolean unique
    ) {
        super(Constants.COLUMN_ANNOTATION_FQN);
        this.name = name;
        this.schema = schema;
        this.table = table;
        this.index = index;
        this.nullable = nullable;
        this.unique = unique;
    }

    public @NotNull String getName() {
        return name;
    }

    public @NotNull String getSchema(@NotNull ParsedDataAnnotation dataAnnotation) {
        if (schema != null) {
            return schema;
        }
        return dataAnnotation.getSchema();
    }

    public @NotNull String getTable(@NotNull ParsedDataAnnotation dataAnnotation) {
        if (table != null) {
            return table;
        }
        return dataAnnotation.getTable();
    }

    public boolean createIndex() {
        return index;
    }

    public boolean isNullable() {
        return nullable;
    }

    public boolean isUnique() {
        return unique;
    }
}
