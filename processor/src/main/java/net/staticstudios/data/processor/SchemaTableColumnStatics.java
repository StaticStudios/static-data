package net.staticstudios.data.processor;

import com.palantir.javapoet.ClassName;
import com.palantir.javapoet.FieldSpec;
import com.palantir.javapoet.TypeSpec;

import javax.lang.model.element.Modifier;

record SchemaTableColumnStatics(String schemaFieldName, String tableFieldName, String columnFieldName) {
    public static SchemaTableColumnStatics generateSchemaTableColumnStatics(TypeSpec.Builder builderType, PersistentValueMetadata persistentValueMetadata) {
        String schemaFieldName = persistentValueMetadata.fieldName() + "$schema";
        String tableFieldName = persistentValueMetadata.fieldName() + "$table";
        String columnFieldName = persistentValueMetadata.fieldName() + "$column";

        // since we support env variables in the name, parse these at runtime.
        builderType.addField(FieldSpec.builder(String.class, schemaFieldName, Modifier.PRIVATE, Modifier.FINAL, Modifier.STATIC)
                .initializer("$T.parseValue($S)", ClassName.get("net.staticstudios.data.util", "ValueUtils"), persistentValueMetadata.schema())
                .build());
        builderType.addField(FieldSpec.builder(String.class, tableFieldName, Modifier.PRIVATE, Modifier.FINAL, Modifier.STATIC)
                .initializer("$T.parseValue($S)", ClassName.get("net.staticstudios.data.util", "ValueUtils"), persistentValueMetadata.table())
                .build());
        builderType.addField(FieldSpec.builder(String.class, columnFieldName, Modifier.PRIVATE, Modifier.FINAL, Modifier.STATIC)
                .initializer("$T.parseValue($S)", ClassName.get("net.staticstudios.data.util", "ValueUtils"), persistentValueMetadata.column())
                .build());

        return new SchemaTableColumnStatics(schemaFieldName, tableFieldName, columnFieldName);
    }
}
