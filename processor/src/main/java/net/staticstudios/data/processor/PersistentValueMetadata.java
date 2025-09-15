package net.staticstudios.data.processor;

import com.palantir.javapoet.TypeName;

public class PersistentValueMetadata implements Metadata {
    private final String schema;
    private final String table;
    private final String column;
    private final String fieldName;
    private final TypeName genericType;

    public PersistentValueMetadata(String schema, String table, String column, String fieldName,
                                   TypeName genericType) {
        this.schema = schema;
        this.table = table;
        this.column = column;
        this.fieldName = fieldName;
        this.genericType = genericType;
    }

    public String schema() {
        return schema;
    }

    public String table() {
        return table;
    }

    public String column() {
        return column;
    }

    public String fieldName() {
        return fieldName;
    }

    public TypeName genericType() {
        return genericType;
    }
}
