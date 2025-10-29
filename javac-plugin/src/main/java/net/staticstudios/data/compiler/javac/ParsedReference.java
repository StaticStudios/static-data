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

class ParsedReference {
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

//    private void settings(That t) {
//        //this method just needs to set the id column values. the column names will be known at compile time
//        //then during insert() is where it get tricky, but create a dummy instance to get the runtime type and them set the values accordingly, in the proper table. table/schema is obtained from runtime type metadata
//
//
//        //todo: if the referenced type is abstract, dont support setting in the builder.
//        String schema; //lookup at runtime due to inheritance
//        String table; //lookup at runtime due to inheritance
////        String[] linkingColumnsInReferringTable = referenceLinkingColumns_[fieldName]; //todo: we dont need this here but we will need it during insert()
//        String[] linkingColumnsInReferencedTable = referenceLinkedColumns_[fieldName];
//
//        for (int i = 0; i < linkingColumnsInReferringTable.length; i++) {
////            String colInReferringTable = linkingColumnsInReferringTable[i];
//            String colInReferencedTable = linkingColumnsInReferencedTable[i];
//            this.refenceLinkingValues_[fieldName] = new Object[]...
//        }
//    }
    //todo: when storing the links, we can have two cols. each a static string array storing the parsed columns. we know the linking columns, and the table schema/table from the reference itself. (what if the refernced data is abstract?
}
