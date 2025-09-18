package net.staticstudios.data.processor;

import com.palantir.javapoet.TypeName;

import java.util.Map;

public class ForeignPersistentValueMetadata extends PersistentValueMetadata {
    private final Map<String, String> links;

    public ForeignPersistentValueMetadata(String schema, String table, String column, String fieldName, TypeName genericType, boolean nullable, Map<String, String> links) {
        super(schema, table, column, fieldName, genericType, nullable);
        this.links = links;
    }

    public Map<String, String> links() {
        return links;
    }
}
