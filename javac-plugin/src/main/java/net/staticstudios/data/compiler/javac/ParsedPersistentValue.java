package net.staticstudios.data.compiler.javac;

import com.sun.tools.javac.code.Attribute;
import com.sun.tools.javac.code.Symbol;
import com.sun.tools.javac.tree.JCTree;
import com.sun.tools.javac.tree.TreeMaker;
import com.sun.tools.javac.util.Names;
import net.staticstudios.data.utils.Constants;
import net.staticstudios.data.utils.Link;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

public class ParsedPersistentValue {
    private final String fieldName;
    private final String schema;
    private final String table;
    private final String column;
    private final boolean nullable;
    private final JCTree.JCExpression type;

    public ParsedPersistentValue(String fieldName, String schema, String table, String column, boolean nullable, JCTree.JCExpression type) {
        this.fieldName = fieldName;
        this.schema = schema;
        this.table = table;
        this.column = column;
        this.nullable = nullable;
        this.type = type;
    }

    public static Collection<ParsedPersistentValue> extractPersistentValues(@NotNull JCTree.JCClassDecl dataClassDecl,
                                                                            @NotNull ParsedDataAnnotation dataAnnotation,
                                                                            @NotNull TreeMaker treeMaker,
                                                                            @NotNull Names names

    ) {
        Collection<ParsedPersistentValue> persistentValues = new ArrayList<>();
        Collection<Symbol.VarSymbol> fields = JavaCPluginUtils.getFields(dataClassDecl, Constants.PERSISTENT_VALUE_FQN);
        for (Symbol.VarSymbol varSymbol : fields) {
            List<Attribute.Compound> annotations = varSymbol.getAnnotationMirrors();
            for (Attribute.Compound annotation : annotations) {
                boolean isIdColumnAnnotation = JavaCPluginUtils.isAnnotation(annotation, Constants.ID_COLUMN_ANNOTATION_FQN);
                boolean isColumnAnnotation = JavaCPluginUtils.isAnnotation(annotation, Constants.COLUMN_ANNOTATION_FQN);
                boolean isForeignColumnAnnotation = JavaCPluginUtils.isAnnotation(annotation, Constants.FOREIGN_COLUMN_ANNOTATION_FQN);

                if (!isColumnAnnotation && !isForeignColumnAnnotation && !isIdColumnAnnotation) {
                    continue;
                }
                String columnName = Objects.requireNonNull(JavaCPluginUtils.getStringAnnotationValue(annotation, "name"));
                String schemaValue;
                String tableValue;
                boolean nullable;

                if (isIdColumnAnnotation) {
                    schemaValue = dataAnnotation.getSchema();
                    tableValue = dataAnnotation.getTable();
                    nullable = false;
                } else {
                    if (isForeignColumnAnnotation) {
                        schemaValue = JavaCPluginUtils.getStringAnnotationValue(annotation, "schema");
                        tableValue = JavaCPluginUtils.getStringAnnotationValue(annotation, "table");
                        if (schemaValue == null) {
                            schemaValue = dataAnnotation.getSchema();
                        }
                        if (tableValue == null) {
                            tableValue = dataAnnotation.getTable();
                        }
                    } else {
                        schemaValue = dataAnnotation.getSchema();
                        tableValue = dataAnnotation.getTable();
                    }

                    nullable = JavaCPluginUtils.getBooleanAnnotationValue(annotation, "nullable");
                }

                JCTree.JCExpression typeExpression = JavaCPluginUtils.getGenericTypeExpression(treeMaker, names, varSymbol, 0);
                ParsedPersistentValue parsedPersistentValue;

                if (isForeignColumnAnnotation) {
                    String insertStrategy = JavaCPluginUtils.getStringAnnotationValue(annotations, Constants.INSERT_ANNOTATION_FQN, "value");
                    if (insertStrategy == null) {
                        insertStrategy = "PREFER_EXISTING";
                    }
                    parsedPersistentValue = new ParsedForeignPersistentValue(
                            varSymbol.getSimpleName().toString(),
                            schemaValue,
                            tableValue,
                            columnName,
                            nullable,
                            typeExpression,
                            insertStrategy,
                            Link.parseRawLinks(JavaCPluginUtils.getStringAnnotationValue(annotation, "link"))
                    );
                } else {
                    parsedPersistentValue = new ParsedPersistentValue(
                            varSymbol.getSimpleName().toString(),
                            schemaValue,
                            tableValue,
                            columnName,
                            nullable,
                            typeExpression
                    );
                }
                persistentValues.add(parsedPersistentValue);
                break;
            }
        }

        return persistentValues;
    }

    public String getFieldName() {
        return fieldName;
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

    public JCTree.JCExpression getType() {
        return type;
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
