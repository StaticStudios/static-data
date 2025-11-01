package net.staticstudios.data.compiler.javac;

import com.sun.tools.javac.code.Flags;
import com.sun.tools.javac.tree.JCTree;
import com.sun.tools.javac.tree.TreeMaker;
import com.sun.tools.javac.util.List;
import com.sun.tools.javac.util.Names;
import net.staticstudios.data.utils.Link;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;

public abstract class AbstractBuilderProcessor {
    protected final JCTree.JCCompilationUnit compilationUnit;
    protected final TreeMaker treeMaker;
    protected final Names names;
    protected final JCTree.JCClassDecl dataClassDecl;
    protected final ParsedDataAnnotation dataAnnotation;
    private final String builderClassSuffix;
    private final @Nullable String builderMethodName;
    protected JCTree.JCClassDecl builderClassDecl;

    public AbstractBuilderProcessor(JCTree.JCCompilationUnit compilationUnit,
                                    TreeMaker treeMaker,
                                    Names names,
                                    JCTree.JCClassDecl dataClassDecl,
                                    ParsedDataAnnotation dataAnnotation, String builderClassSuffix,
                                    @Nullable String builderMethodName
    ) {
        this.compilationUnit = compilationUnit;
        this.treeMaker = treeMaker;
        this.names = names;
        this.dataClassDecl = dataClassDecl;
        this.dataAnnotation = dataAnnotation;
        this.builderClassSuffix = builderClassSuffix;
        this.builderMethodName = builderMethodName;
    }

    protected abstract void addImports();

    protected abstract void process();

    public void runProcessor() {
        if (dataClassDecl.defs.stream()
                .anyMatch(def -> def instanceof JCTree.JCClassDecl &&
                        ((JCTree.JCClassDecl) def).name.toString().equals(getBuilderClassName()))) {
            return;
        }

        JavaCPluginUtils.importClass(compilationUnit, treeMaker, names, "net.staticstudios.data", "DataManager");

        addImports();
        makeBuilderClass();
        if (builderMethodName != null) {
            makeBuilderMethod();
            makeParameterizedBuilderMethod();
        }
        process();
    }

    protected @Nullable SuperClass extending() {
        return null;
    }

    protected void makeBuilderClass() {
        SuperClass superClass = extending();
        JCTree.JCExpression classExtends;
        JCTree.JCExpression superCall;
        if (superClass != null) {
            classExtends = treeMaker.TypeApply(
                    treeMaker.Ident(names.fromString(superClass.simpleName())),
                    superClass.superParms()
            );
            superCall = treeMaker.Apply(
                    List.nil(),
                    treeMaker.Ident(names.fromString("super")),
                    superClass.args()
            );
        } else {
            classExtends = null;
            superCall = null;
        }

        builderClassDecl = treeMaker.ClassDef(
                treeMaker.Modifiers(Flags.PUBLIC | Flags.STATIC),
                names.fromString(getBuilderClassName()),
                List.nil(),
                classExtends,
                List.nil(),
                List.nil()
        );


        java.util.List<JCTree.JCExpressionStatement> constructorBodyStatements = new ArrayList<>();
        if (superCall != null) {
            constructorBodyStatements.add(treeMaker.Exec(superCall));
        }

        if (this.builderMethodName != null) {
            JCTree.JCVariableDecl dataManagerField = treeMaker.VarDef(
                    treeMaker.Modifiers(Flags.PRIVATE | Flags.FINAL),
                    names.fromString("dataManager"),
                    treeMaker.Ident(names.fromString("DataManager")),
                    null
            );
            builderClassDecl.defs = builderClassDecl.defs.append(dataManagerField);
            constructorBodyStatements.add(
                    treeMaker.Exec(
                            treeMaker.Assign(
                                    treeMaker.Select(
                                            treeMaker.Ident(names.fromString("this")),
                                            names.fromString("dataManager")
                                    ),
                                    treeMaker.Ident(names.fromString("dataManager"))
                            )
                    )
            );
        }

        JCTree.JCMethodDecl constructor = treeMaker.MethodDef(
                treeMaker.Modifiers(Flags.PUBLIC),
                names.fromString("<init>"),
                null,
                List.nil(),
                this.builderMethodName == null ?
                        List.nil()
                        :
                        List.of(
                                treeMaker.VarDef(
                                        treeMaker.Modifiers(Flags.PARAMETER),
                                        names.fromString("dataManager"),
                                        treeMaker.Ident(names.fromString("DataManager")),
                                        null
                                )
                        ),
                List.nil(),
                treeMaker.Block(0, List.from(constructorBodyStatements)),
                null
        );
        builderClassDecl.defs = builderClassDecl.defs.append(constructor);

        dataClassDecl.defs = dataClassDecl.defs.append(builderClassDecl);
    }

    private void makeParameterizedBuilderMethod() {
        JCTree.JCMethodDecl builderMethod = treeMaker.MethodDef(
                treeMaker.Modifiers(Flags.PUBLIC | Flags.STATIC),
                names.fromString(builderMethodName),
                treeMaker.Ident(names.fromString(getBuilderClassName())),
                List.nil(),
                List.nil(),
                List.nil(),
                treeMaker.Block(0, List.of(
                        treeMaker.Return(
                                treeMaker.Apply(
                                        List.nil(),
                                        treeMaker.Ident(names.fromString(builderMethodName)),
                                        List.of(
                                                treeMaker.Apply(
                                                        List.nil(),
                                                        treeMaker.Select(
                                                                treeMaker.Ident(names.fromString("DataManager")),
                                                                names.fromString("getInstance")
                                                        ),
                                                        List.nil()
                                                )
                                        )
                                )
                        )
                )),
                null
        );
        dataClassDecl.defs = dataClassDecl.defs.append(builderMethod);
    }

    private void makeBuilderMethod() {
        JCTree.JCMethodDecl builderMethod = treeMaker.MethodDef(
                treeMaker.Modifiers(Flags.PUBLIC | Flags.STATIC),
                names.fromString(builderMethodName),
                treeMaker.Ident(names.fromString(getBuilderClassName())),
                List.nil(),
                List.of(
                        treeMaker.VarDef(
                                treeMaker.Modifiers(Flags.PARAMETER),
                                names.fromString("dataManager"),
                                treeMaker.Ident(names.fromString("DataManager")),
                                null
                        )
                ),
                List.nil(),
                treeMaker.Block(0, List.of(
                        treeMaker.Return(
                                treeMaker.NewClass(null, List.nil(),
                                        treeMaker.Ident(names.fromString(getBuilderClassName())),
                                        List.of(
                                                treeMaker.Ident(names.fromString("dataManager"))
                                        ),
                                        null
                                )
                        )
                )),
                null
        );
        dataClassDecl.defs = dataClassDecl.defs.append(builderMethod);
    }

    public String getBuilderClassName() {
        return dataClassDecl.name.toString() + builderClassSuffix;
    }

    public String storeSchema(String fieldName, String encoded) {
        String schemaFieldName = getStoredSchemaFieldName(fieldName);
        JavaCPluginUtils.generatePrivateStaticField(treeMaker, names, builderClassDecl, schemaFieldName, treeMaker.Ident(names.fromString("String")),
                treeMaker.Apply(
                        List.nil(),
                        treeMaker.Select(
                                treeMaker.Ident(names.fromString("ValueUtils")),
                                names.fromString("parseValue")
                        ),
                        List.of(
                                treeMaker.Literal(encoded)
                        )
                )
        );
        return schemaFieldName;
    }

    public String storeTable(String fieldName, String encoded) {
        String tableFieldName = getStoredTableFieldName(fieldName);
        JavaCPluginUtils.generatePrivateStaticField(treeMaker, names, builderClassDecl, tableFieldName, treeMaker.Ident(names.fromString("String")),
                treeMaker.Apply(
                        List.nil(),
                        treeMaker.Select(
                                treeMaker.Ident(names.fromString("ValueUtils")),
                                names.fromString("parseValue")
                        ),
                        List.of(
                                treeMaker.Literal(encoded)
                        )
                )
        );
        return tableFieldName;
    }

    public String storeColumn(String fieldName, String encoded) {
        String columnFieldName = getStoredColumnFieldName(fieldName);
        JavaCPluginUtils.generatePrivateStaticField(treeMaker, names, builderClassDecl, columnFieldName, treeMaker.Ident(names.fromString("String")),
                treeMaker.Apply(
                        List.nil(),
                        treeMaker.Select(
                                treeMaker.Ident(names.fromString("ValueUtils")),
                                names.fromString("parseValue")
                        ),
                        List.of(
                                treeMaker.Literal(encoded)
                        )
                )
        );
        return columnFieldName;
    }

    public String getStoredSchemaFieldName(String fieldName) {
        return fieldName + "$schema";
    }

    public String getStoredTableFieldName(String fieldName) {
        return fieldName + "$table";
    }

    public String getStoredColumnFieldName(String fieldName) {
        return fieldName + "$column";
    }

    public void storeLinks(String fieldName, java.util.List<Link> links) {
        String referringColumnsFieldName = fieldName + "$referringColumns";
        String referencedColumnsFieldName = fieldName + "$referencedColumns";
        JavaCPluginUtils.generatePrivateStaticField(treeMaker, names, builderClassDecl, referringColumnsFieldName, treeMaker.TypeArray(treeMaker.Ident(names.fromString("String"))),
                treeMaker.NewArray(
                        treeMaker.Ident(names.fromString("String")),
                        List.nil(),
                        List.from(
                                links.stream().map(link ->
                                        treeMaker.Literal(link.columnInReferringTable())
                                ).toList()
                        )
                )
        );

        JavaCPluginUtils.generatePrivateStaticField(treeMaker, names, builderClassDecl, referencedColumnsFieldName, treeMaker.TypeArray(treeMaker.Ident(names.fromString("String"))),
                treeMaker.NewArray(
                        treeMaker.Ident(names.fromString("String")),
                        List.nil(),
                        List.from(
                                links.stream().map(link ->
                                        treeMaker.Literal(link.columnInReferencedTable())
                                ).toList()
                        )
                )
        );
    }

    public String getStoredReferringColumnsFieldName(String fieldName) {
        return fieldName + "$referringColumns";
    }

    public String getStoredReferencedColumnsFieldName(String fieldName) {
        return fieldName + "$referencedColumns";
    }
}
