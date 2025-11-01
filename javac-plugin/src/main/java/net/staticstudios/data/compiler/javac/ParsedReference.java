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

public class ParsedReference {
    private final String fieldName;
    private final List<Link> links;
    private final JCTree.JCExpression type;

    public ParsedReference(String fieldName, List<Link> links, JCTree.JCExpression type) {
        this.fieldName = fieldName;
        this.links = links;
        this.type = type;
    }

    public static Collection<ParsedReference> extractReferences(@NotNull JCTree.JCClassDecl dataClassDecl,
                                                                @NotNull ParsedDataAnnotation dataAnnotation,
                                                                @NotNull TreeMaker treeMaker,
                                                                @NotNull Names names

    ) {
        Collection<ParsedReference> references = new ArrayList<>();
        Collection<Symbol.VarSymbol> fields = JavaCPluginUtils.getFields(dataClassDecl, Constants.REFERENCE_FQN);
        for (Symbol.VarSymbol varSymbol : fields) {
            List<Attribute.Compound> annotations = varSymbol.getAnnotationMirrors();
            for (Attribute.Compound annotation : annotations) {
                boolean isOneToOneAnnotation = JavaCPluginUtils.isAnnotation(annotation, Constants.ONE_TO_ONE_ANNOTATION_FQN);

                if (!isOneToOneAnnotation) {
                    continue;
                }

                List<Link> links = Link.parseRawLinks(JavaCPluginUtils.getStringAnnotationValue(annotation, "link"));

                JCTree.JCExpression typeExpression = JavaCPluginUtils.getGenericTypeExpression(treeMaker, names, varSymbol, 0);
                ParsedReference parsedReference = new ParsedReference(
                        varSymbol.getSimpleName().toString(),
                        links,
                        typeExpression
                );
                references.add(parsedReference);
                break;
            }
        }

        return references;
    }

    public String getFieldName() {
        return fieldName;
    }

    public List<Link> getLinks() {
        return links;
    }

    public JCTree.JCExpression getType() {
        return type;
    }

    @Override
    public String toString() {
        return "ParsedReference{" +
                "fieldName='" + fieldName + '\'' +
                ", links=" + links +
                ", type=" + type +
                '}';
    }
}
