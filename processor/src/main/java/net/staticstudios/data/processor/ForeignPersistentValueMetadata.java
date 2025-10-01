package net.staticstudios.data.processor;

import com.palantir.javapoet.TypeName;
import net.staticstudios.data.InsertStrategy;

import java.util.Map;

public class ForeignPersistentValueMetadata extends PersistentValueMetadata {
    private final Map<String, String> links;
    private final InsertStrategy insertStrategy;

    public ForeignPersistentValueMetadata(String schema, String table, String column, String fieldName, TypeName genericType, boolean nullable, Map<String, String> links, InsertStrategy insertStrategy) {
        super(schema, table, column, fieldName, genericType, nullable);
        this.links = links;
        this.insertStrategy = insertStrategy;
    }

    public Map<String, String> links() {
        return links;
    }

    public InsertStrategy insertStrategy() {
        return insertStrategy;
    }
}
