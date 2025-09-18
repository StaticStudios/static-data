package net.staticstudios.data.processor;

import com.palantir.javapoet.TypeName;

public class PersistentValueMetadata implements Metadata {
    private final String schema;
    private final String table;
    private final String column;
    private final String fieldName;
    private final TypeName genericType;
    private final boolean nullable;


    public PersistentValueMetadata(String schema, String table, String column, String fieldName,
                                   TypeName genericType, boolean nullable) {
        this.schema = schema;
        this.table = table;
        this.column = column;
        this.fieldName = fieldName;
        this.genericType = genericType;
        this.nullable = nullable;
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

    public boolean nullable() {
        return nullable;
    }

    public TypeName genericType() {
        return genericType;
    }
}
