package net.staticstudios.data.compiler.javac.javac;

import net.staticstudios.data.*;
import net.staticstudios.data.compiler.javac.util.SimpleField;
import net.staticstudios.data.compiler.javac.util.TypeUtils;
import net.staticstudios.data.utils.Constants;
import net.staticstudios.data.utils.Link;
import org.jetbrains.annotations.NotNull;

import javax.lang.model.element.Element;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeMirror;
import java.util.ArrayList;
import java.util.Collection;

public class ParsedPersistentValue extends ParsedValue {
    private final String schema;
    private final String table;
    private final String column;
    private final boolean nullable;

    public ParsedPersistentValue(String fieldName, String schema, String table, String column, boolean nullable, TypeElement type) {
        super(fieldName, type);
        this.schema = schema;
        this.table = table;
        this.column = column;
        this.nullable = nullable;
    }

    public static Collection<ParsedPersistentValue> extractPersistentValues(@NotNull TypeElement dataClass,
                                                                            @NotNull Data dataAnnotation,
                                                                            @NotNull TypeUtils typeUtils

    ) {
        Collection<ParsedPersistentValue> persistentValues = new ArrayList<>();
        Collection<SimpleField> fields = typeUtils.getFields(dataClass, Constants.PERSISTENT_VALUE_FQN);
        for (SimpleField pvField : fields) {
            Element fieldElement = pvField.element();
            Column columnAnnotation = fieldElement.getAnnotation(Column.class);
            IdColumn idColumnAnnotation = fieldElement.getAnnotation(IdColumn.class);
            ForeignColumn foreignColumnAnnotation = fieldElement.getAnnotation(ForeignColumn.class);
            if (columnAnnotation == null && idColumnAnnotation == null && foreignColumnAnnotation == null) {
                continue;
            }

            String columnName;
            String schemaValue;
            String tableValue;
            boolean nullable;

            if (idColumnAnnotation != null) {
                columnName = idColumnAnnotation.name();
                schemaValue = dataAnnotation.schema();
                tableValue = dataAnnotation.table();
                nullable = false;
            } else if (foreignColumnAnnotation != null) {
                columnName = foreignColumnAnnotation.name();
                schemaValue = foreignColumnAnnotation.schema().isEmpty() ? dataAnnotation.schema() : foreignColumnAnnotation.schema();
                tableValue = foreignColumnAnnotation.table().isEmpty() ? dataAnnotation.table() : foreignColumnAnnotation.table();
                nullable = foreignColumnAnnotation.nullable();


            } else {
                columnName = columnAnnotation.name();
                schemaValue = dataAnnotation.schema();
                tableValue = dataAnnotation.table();
                nullable = columnAnnotation.nullable();
            }

            TypeMirror genericTypeMirror = typeUtils.getGenericType(fieldElement, 0);
            TypeElement typeElement = (TypeElement) ((DeclaredType) genericTypeMirror).asElement();
            ParsedPersistentValue persistentValue;

            if (foreignColumnAnnotation != null) {
                InsertStrategy insertStrategy = null;
                Insert insertAnnotation = fieldElement.getAnnotation(Insert.class);
                if (insertAnnotation != null) {
                    insertStrategy = insertAnnotation.value();
                }


                persistentValue = new ParsedForeignPersistentValue(
                        pvField.name(),
                        schemaValue,
                        tableValue,
                        columnName,
                        nullable,
                        typeElement,
                        insertStrategy,
                        Link.parseRawLinks(foreignColumnAnnotation.link())
                );
            } else {
                persistentValue = new ParsedPersistentValue(
                        pvField.name(),
                        schemaValue,
                        tableValue,
                        columnName,
                        nullable,
                        typeElement
                );
            }

            persistentValues.add(persistentValue);

        }

        return persistentValues;
    }

    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
    }

    public String getColumn() {
        return column;
    }

    public boolean isNullable() {
        return nullable;
    }


    public String[] getTypeFQNParts() {
        return type.getQualifiedName().toString().split("\\.");
    }

    @Override
    public String toString() {
        return "PersistentValue{" +
                "fieldName='" + fieldName + '\'' +
                ", schema='" + schema + '\'' +
                ", table='" + table + '\'' +
                ", column='" + column + '\'' +
                ", type=" + type +
                '}';
    }
}
