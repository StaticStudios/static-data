package net.staticstudios.data.compiler.javac.javac;

import net.staticstudios.data.Data;
import net.staticstudios.data.Identifier;
import net.staticstudios.data.compiler.javac.util.SimpleField;
import net.staticstudios.data.compiler.javac.util.TypeUtils;
import net.staticstudios.data.utils.Constants;
import org.jetbrains.annotations.NotNull;

import javax.lang.model.element.Element;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeMirror;
import java.util.ArrayList;
import java.util.Collection;

public class ParsedCachedValue extends ParsedValue {
    private final String schema;
    private final String table;
    private final String identifier;

    public ParsedCachedValue(String fieldName, String schema, String table, String identifier, TypeElement type) {
        super(fieldName, type);
        this.schema = schema;
        this.table = table;
        this.identifier = identifier;
    }

    public static Collection<ParsedCachedValue> extractCachedValues(@NotNull TypeElement dataClass,
                                                                    @NotNull Data dataAnnotation,
                                                                    @NotNull TypeUtils typeUtils

    ) {
        Collection<ParsedCachedValue> cachedValues = new ArrayList<>();
        Collection<SimpleField> fields = typeUtils.getFields(dataClass, Constants.CACHED_VALUE_FQN);
        for (SimpleField pvField : fields) {
            Element fieldElement = pvField.element();
            Identifier identifierAnnotation = fieldElement.getAnnotation(Identifier.class);
            if (identifierAnnotation == null) {
                continue;
            }

            String schemaValue = dataAnnotation.schema();
            String tableValue = dataAnnotation.table();
            String identifierValue = identifierAnnotation.value();

            TypeMirror genericTypeMirror = typeUtils.getGenericType(fieldElement, 0);
            TypeElement typeElement = (TypeElement) ((DeclaredType) genericTypeMirror).asElement();
            ParsedCachedValue persistentValue = new ParsedCachedValue(
                    pvField.name(),
                    schemaValue,
                    tableValue,
                    identifierValue,
                    typeElement
            );

            cachedValues.add(persistentValue);

        }

        return cachedValues;
    }

    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
    }

    public String getIdentifier() {
        return identifier;
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
                ", identifier='" + identifier + '\'' +
                ", type=" + type +
                '}';
    }
}
