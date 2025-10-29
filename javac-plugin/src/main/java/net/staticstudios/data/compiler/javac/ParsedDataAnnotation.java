package net.staticstudios.data.compiler.javac;

import com.google.common.base.Preconditions;
import com.sun.tools.javac.tree.JCTree;
import net.staticstudios.data.utils.Constants;
import org.jetbrains.annotations.NotNull;

public class ParsedDataAnnotation extends ParsedAnnotation {
    private final @NotNull String schema;
    private final @NotNull String table;

    public ParsedDataAnnotation(@NotNull String schema, @NotNull String table) {
        super(Constants.DATA_ANNOTATION_FQN);
        this.schema = schema;
        this.table = table;
    }

    public static ParsedDataAnnotation extract(JCTree.JCClassDecl classDecl) {
        JCTree.JCAnnotation dataAnnotation = JavaCPluginUtils.extractAnnotation(classDecl, Constants.DATA_ANNOTATION_FQN);
        Preconditions.checkNotNull(dataAnnotation, "Data annotation not found on class: " + classDecl.getSimpleName());
        String schema = JavaCPluginUtils.getStringAnnotationValue(dataAnnotation, "schema");
        String table = JavaCPluginUtils.getStringAnnotationValue(dataAnnotation, "table");

        Preconditions.checkNotNull(schema, "Data annotation 'schema' value cannot be null on class: " + classDecl.getSimpleName());
        Preconditions.checkNotNull(table, "Data annotation 'table' value cannot be null on class: " + classDecl.getSimpleName());

        return new ParsedDataAnnotation(schema, table);
    }

    public @NotNull String getSchema() {
        return schema;
    }

    public @NotNull String getTable() {
        return table;
    }
}
