package net.staticstudios.data.processor;

import com.palantir.javapoet.ClassName;
import com.palantir.javapoet.FieldSpec;
import com.palantir.javapoet.TypeName;
import com.palantir.javapoet.TypeSpec;
import net.staticstudios.data.*;
import net.staticstudios.data.utils.StringUtils;

import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.ElementFilter;
import java.util.*;

public class MetadataUtils {
    private static final String FQN_OBJECT = Object.class.getName();
    private static final String FQN_PERSISTENT_VALUE = "net.staticstudios.data.PersistentValue";

    public static List<Metadata> extractMetadata(TypeElement typeElement) {
        Data dataAnnotation = typeElement.getAnnotation(Data.class);
        if (dataAnnotation == null) {
            return Collections.emptyList();
        }


        List<Metadata> metadata = new ArrayList<>();
        extractMetadata(dataAnnotation, metadata, typeElement);

        return metadata;
    }

    private static void extractMetadata(Data dataAnnotation, List<Metadata> list, TypeElement typeElement) {
        for (VariableElement field : ElementFilter.fieldsIn(typeElement.getEnclosedElements())) {
            if (field.getModifiers().contains(Modifier.STATIC)) {
                continue;
            }

            TypeMirror mirror = field.asType();
            if (!(mirror instanceof DeclaredType declaredType)) {
                continue;
            }
            TypeElement fieldTypeElement = (TypeElement) declaredType.asElement();
            if (fieldTypeElement.getQualifiedName().toString().equals(FQN_PERSISTENT_VALUE)) {
                TypeMirror innerType = declaredType.getTypeArguments().getFirst();
                PersistentValueMetadata metadata = getPersistentValueMetadata(dataAnnotation, field, TypeName.get(innerType));
                list.add(metadata);
            }
        }

        TypeMirror superClass = typeElement.getSuperclass();
        if (superClass instanceof DeclaredType declaredSuper && declaredSuper.asElement() instanceof TypeElement superElement) {
            if (superClass.getKind() != TypeKind.NONE && superClass.getKind() != TypeKind.VOID && !superElement.getQualifiedName().toString().equals(FQN_OBJECT)) {
                extractMetadata(dataAnnotation, list, superElement);
            }
        }
    }

    private static PersistentValueMetadata getPersistentValueMetadata(Data dataAnnotation, VariableElement field, TypeName typeName) {
        String schemaName = null;
        String tableName = null;
        String columnName = null;
        boolean nullable = false;

        IdColumn idColumn = field.getAnnotation(IdColumn.class);
        Column column = field.getAnnotation(Column.class);
        ForeignColumn foreignColumn = field.getAnnotation(ForeignColumn.class);
        Insert insert = field.getAnnotation(Insert.class);

        if (idColumn != null) {
            tableName = dataAnnotation.table();
            schemaName = dataAnnotation.schema();
            columnName = idColumn.name();
        } else if (column != null) {
            tableName = dataAnnotation.table();
            schemaName = dataAnnotation.schema();
            columnName = column.name();
            nullable = column.nullable();
        } else if (foreignColumn != null) {
            tableName = foreignColumn.table().isEmpty() ? dataAnnotation.table() : foreignColumn.table();
            schemaName = foreignColumn.schema().isEmpty() ? dataAnnotation.schema() : foreignColumn.schema();
            columnName = foreignColumn.name();
            nullable = foreignColumn.nullable();
        }

        if (idColumn != null || column != null) {
            return new PersistentValueMetadata(
                    schemaName,
                    tableName,
                    columnName,
                    field.getSimpleName().toString(),
                    typeName,
                    nullable
            );
        }
        if (foreignColumn != null) {
            Map<String, String> links = new HashMap<>();
            for (String link : StringUtils.parseCommaSeperatedList(foreignColumn.link())) {
                String[] parts = link.split("=");
                if (parts.length != 2) {
                    throw new IllegalArgumentException("Invalid link format in @ForeignColumn: " + link);
                }
                links.put(parts[0].trim(), parts[1].trim());
            }

            InsertStrategy insertStrategy = insert != null ? insert.value() : InsertStrategy.PREFER_EXISTING;

            return new ForeignPersistentValueMetadata(
                    schemaName,
                    tableName,
                    columnName,
                    field.getSimpleName().toString(),
                    typeName,
                    nullable,
                    links,
                    insertStrategy
            );
        }
        throw new IllegalStateException("Field " + field.getSimpleName() + " is not annotated with @IdColumn, @Column, or @ForeignColumn");
    }

    public static List<ForeignLink> makeFPVStatics(TypeSpec.Builder builder, ForeignPersistentValueMetadata foreignPersistentValueMetadata, List<Metadata> metadataList, Data dataAnnotation, SchemaTableColumnStatics statics) {
        List<ForeignLink> staticNames = new ArrayList<>();
        int i = 0;
        for (Map.Entry<String, String> link : foreignPersistentValueMetadata.links().entrySet()) {
            String localColumn = link.getKey();
            String foreignColumn = link.getValue();

            PersistentValueMetadata localColumnMetadata = metadataList.stream()
                    .filter(m -> m instanceof PersistentValueMetadata)
                    .map(m -> (PersistentValueMetadata) m)
                    .filter(m -> m.column().equals(localColumn) && m.table().equals(dataAnnotation.table()) && m.schema().equals(dataAnnotation.schema()))
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException("Could not find local column metadata for link: " + localColumn));

            String columnLinkFieldName = statics.columnFieldName() + "$" + localColumnMetadata.fieldName() + "$" + i;

            builder.addField(FieldSpec.builder(String.class, columnLinkFieldName, Modifier.PRIVATE, Modifier.FINAL, Modifier.STATIC)
                    .initializer("$T.parseValue($S)", ClassName.get("net.staticstudios.data.util", "ValueUtils"), foreignColumn)
                    .build());

            staticNames.add(new ForeignLink(columnLinkFieldName, localColumnMetadata));
            i++;
        }
        return staticNames;
    }

}
