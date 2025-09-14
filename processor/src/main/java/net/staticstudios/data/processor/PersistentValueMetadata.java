package net.staticstudios.data.processor;

import com.palantir.javapoet.TypeName;

public record PersistentValueMetadata(String schema, String table, String column, String fieldName,
                                      TypeName genericType) implements Metadata {
}
