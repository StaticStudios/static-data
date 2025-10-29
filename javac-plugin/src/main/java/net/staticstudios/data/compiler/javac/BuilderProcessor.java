package net.staticstudios.data.compiler.javac;

import com.sun.tools.javac.code.Flags;
import com.sun.tools.javac.code.TypeTag;
import com.sun.tools.javac.tree.JCTree;
import com.sun.tools.javac.tree.TreeMaker;
import com.sun.tools.javac.util.List;
import com.sun.tools.javac.util.Names;

import java.util.ArrayList;
import java.util.Collection;

public class BuilderProcessor extends AbstractBuilderProcessor {

    public BuilderProcessor(JCTree.JCCompilationUnit compilationUnit, TreeMaker treeMaker, Names names, JCTree.JCClassDecl dataClassDecl, ParsedDataAnnotation dataAnnotation) {
        super(compilationUnit, treeMaker, names, dataClassDecl, dataAnnotation, "Builder", "builder");
    }

    public static boolean hasProcessed(JCTree.JCClassDecl classDecl) {
        return classDecl.defs.stream()
                .anyMatch(def -> def instanceof JCTree.JCClassDecl &&
                        ((JCTree.JCClassDecl) def).name.toString().equals(classDecl.name + "Builder"));
    }

    @Override
    protected void addImports() {
        JavaCPluginUtils.importClass(compilationUnit, treeMaker, names, "net.staticstudios.data.util", "ValueUtils");
        JavaCPluginUtils.importClass(compilationUnit, treeMaker, names, "net.staticstudios.data.insert", "InsertContext");
        JavaCPluginUtils.importClass(compilationUnit, treeMaker, names, "net.staticstudios.data", "InsertMode");
        JavaCPluginUtils.importClass(compilationUnit, treeMaker, names, "net.staticstudios.data", "InsertStrategy");
        JavaCPluginUtils.importClass(compilationUnit, treeMaker, names, "net.staticstudios.data.util", "UniqueDataMetadata");
        JavaCPluginUtils.importClass(compilationUnit, treeMaker, names, "net.staticstudios.data.util", "ColumnValuePair");
    }

    @Override
    protected void process() {
        Collection<ParsedPersistentValue> persistentValues = ParsedPersistentValue.extractPersistentValues(dataClassDecl, dataAnnotation, treeMaker, names);
        for (ParsedPersistentValue pv : persistentValues) {
            processValue(pv);
        }

        Collection<ParsedReference> references = ParsedReference.extractReferences(dataClassDecl, dataAnnotation, treeMaker, names);
        for (ParsedReference ref : references) {
            processReference(ref);
        }

        makeInsertContextMethod(persistentValues, references);
        makeInsertModeMethod();
    }


    private void processValue(ParsedPersistentValue pv) {
        String schemaFieldName = pv.getFieldName() + "$schema";
        String tableFieldName = pv.getFieldName() + "$table";
        String columnFieldName = pv.getFieldName() + "$column";

        JCTree.JCExpression stringType = treeMaker.Ident(names.fromString("String"));
        JCTree.JCExpression schemaInit = treeMaker.Apply(
                List.nil(),
                treeMaker.Select(
                        treeMaker.Ident(names.fromString("ValueUtils")),
                        names.fromString("parseValue")
                ),
                List.of(
                        treeMaker.Literal(pv.getSchema())
                )
        );
        JCTree.JCExpression tableInit = treeMaker.Apply(
                List.nil(),
                treeMaker.Select(
                        treeMaker.Ident(names.fromString("ValueUtils")),
                        names.fromString("parseValue")
                ),
                List.of(
                        treeMaker.Literal(pv.getTable())
                )
        );
        JCTree.JCExpression columnInit = treeMaker.Apply(
                List.nil(),
                treeMaker.Select(
                        treeMaker.Ident(names.fromString("ValueUtils")),
                        names.fromString("parseValue")
                ),
                List.of(
                        treeMaker.Literal(pv.getColumn())
                )
        );

        JavaCPluginUtils.generatePrivateStaticField(treeMaker, names, builderClassDecl, schemaFieldName, stringType, schemaInit);
        JavaCPluginUtils.generatePrivateStaticField(treeMaker, names, builderClassDecl, tableFieldName, stringType, tableInit);
        JavaCPluginUtils.generatePrivateStaticField(treeMaker, names, builderClassDecl, columnFieldName, stringType, columnInit);

        JCTree.JCExpression nullInit = treeMaker.Literal(TypeTag.BOT, null);

        JavaCPluginUtils.generatePrivateMemberField(treeMaker, names, builderClassDecl, pv.getFieldName(), pv.getType(), nullInit);

        JCTree.JCMethodDecl setterMethod = treeMaker.MethodDef(
                treeMaker.Modifiers(Flags.PUBLIC | Flags.FINAL),
                names.fromString(pv.getFieldName()),
                treeMaker.Ident(names.fromString(getBuilderClassName())),
                List.nil(),
                List.of(
                        treeMaker.VarDef(
                                treeMaker.Modifiers(Flags.PARAMETER),
                                names.fromString(pv.getFieldName()),
                                pv.getType(),
                                null
                        )
                ),
                List.nil(),
                treeMaker.Block(0, List.of(
                        treeMaker.Exec(
                                treeMaker.Assign(
                                        treeMaker.Select(
                                                treeMaker.Ident(names.fromString("this")),
                                                names.fromString(pv.getFieldName())
                                        ),
                                        treeMaker.Ident(names.fromString(pv.getFieldName()))
                                )
                        ),
                        treeMaker.Return(
                                treeMaker.Ident(names.fromString("this"))
                        )
                )),
                null
        );

        builderClassDecl.defs = builderClassDecl.defs.append(setterMethod);
    }

    private void processReference(ParsedReference ref) {
        String idColumnValuePairsFieldName = ref.getFieldName() + "_reference$idColumnValuePairs";
        String schemaFieldName = ref.getFieldName() + "_reference$schema";
        String tableFieldName = ref.getFieldName() + "_reference$table";

        JCTree.JCExpression arrayType = treeMaker.TypeArray(treeMaker.Ident(names.fromString("ColumnValuePair")));
        JCTree.JCExpression stringType = treeMaker.Ident(names.fromString("String"));

        JCTree.JCExpression nullInit = treeMaker.Literal(TypeTag.BOT, null);

        JavaCPluginUtils.generatePrivateMemberField(treeMaker, names, builderClassDecl, idColumnValuePairsFieldName, arrayType, nullInit);
        JavaCPluginUtils.generatePrivateMemberField(treeMaker, names, builderClassDecl, schemaFieldName, stringType, nullInit);
        JavaCPluginUtils.generatePrivateMemberField(treeMaker, names, builderClassDecl, tableFieldName, stringType, nullInit);

        var handleNotNull = treeMaker.Block(0, List.of(
                treeMaker.Exec(
                        treeMaker.Assign(
                                treeMaker.Select(
                                        treeMaker.Ident(names.fromString("this")),
                                        names.fromString(idColumnValuePairsFieldName)
                                ),
                                treeMaker.Apply(
                                        List.nil(),
                                        treeMaker.Select(
                                                treeMaker.Apply(
                                                        List.nil(),
                                                        treeMaker.Select(
                                                                treeMaker.Ident(names.fromString(ref.getFieldName())),
                                                                names.fromString("getIdColumns")
                                                        ),
                                                        List.nil()
                                                ),
                                                names.fromString("getPairs")
                                        ),
                                        List.nil()
                                )
                        )
                ),
                treeMaker.VarDef(
                        treeMaker.Modifiers(0),
                        names.fromString("__$metadata"),
                        treeMaker.Ident(names.fromString("UniqueDataMetadata")),
                        treeMaker.Apply(
                                List.nil(),
                                treeMaker.Select(
                                        treeMaker.Ident(names.fromString(ref.getFieldName())),
                                        names.fromString("getMetadata")
                                ),
                                List.nil()
                        )
                ),
                treeMaker.Exec(
                        treeMaker.Assign(
                                treeMaker.Select(
                                        treeMaker.Ident(names.fromString("this")),
                                        names.fromString(schemaFieldName)
                                ),
                                treeMaker.Apply(
                                        List.nil(),
                                        treeMaker.Select(
                                                treeMaker.Ident(names.fromString("__$metadata")),
                                                names.fromString("schema")
                                        ),
                                        List.nil()
                                )
                        )
                ),
                treeMaker.Exec(
                        treeMaker.Assign(
                                treeMaker.Select(
                                        treeMaker.Ident(names.fromString("this")),
                                        names.fromString(tableFieldName)
                                ),
                                treeMaker.Apply(
                                        List.nil(),
                                        treeMaker.Select(
                                                treeMaker.Ident(names.fromString("__$metadata")),
                                                names.fromString("table")
                                        ),
                                        List.nil()
                                )
                        )
                )
        ));

        var handleNull = treeMaker.Block(0, List.of(
                treeMaker.Exec(
                        treeMaker.Assign(
                                treeMaker.Select(
                                        treeMaker.Ident(names.fromString("this")),
                                        names.fromString(idColumnValuePairsFieldName)
                                ),
                                treeMaker.Literal(TypeTag.BOT, null)
                        )
                ),
                treeMaker.Exec(
                        treeMaker.Assign(
                                treeMaker.Select(
                                        treeMaker.Ident(names.fromString("this")),
                                        names.fromString(schemaFieldName)
                                ),
                                treeMaker.Literal(TypeTag.BOT, null)
                        )
                ),
                treeMaker.Exec(
                        treeMaker.Assign(
                                treeMaker.Select(
                                        treeMaker.Ident(names.fromString("this")),
                                        names.fromString(tableFieldName)
                                ),
                                treeMaker.Literal(TypeTag.BOT, null)
                        )
                )
        ));

        JCTree.JCMethodDecl setterMethod = treeMaker.MethodDef(
                treeMaker.Modifiers(Flags.PUBLIC | Flags.FINAL),
                names.fromString(ref.getFieldName()),
                treeMaker.Ident(names.fromString(getBuilderClassName())),
                List.nil(),
                List.of(
                        treeMaker.VarDef(
                                treeMaker.Modifiers(Flags.PARAMETER),
                                names.fromString(ref.getFieldName()),
                                ref.getType(),
                                null
                        )
                ),
                List.nil(),
                treeMaker.Block(0, List.of(
                        treeMaker.If(
                                treeMaker.Binary(
                                        JCTree.Tag.NE,
                                        treeMaker.Ident(names.fromString(ref.getFieldName())),
                                        treeMaker.Literal(TypeTag.BOT, null)
                                ),
                                handleNotNull,
                                handleNull
                        ),
                        treeMaker.Return(
                                treeMaker.Ident(names.fromString("this"))
                        )
                )),
                null
        );
        builderClassDecl.defs = builderClassDecl.defs.append(setterMethod);
    }

    private void makeInsertContextMethod(Collection<ParsedPersistentValue> parsedPersistentValues, Collection<ParsedReference> parsedReferences) {
        java.util.List<JCTree.JCStatement> bodyStatements = new ArrayList<>();

        for (ParsedPersistentValue pv : parsedPersistentValues) {
            JCTree.JCExpression schemaFieldAccess = treeMaker.Ident(names.fromString(pv.getFieldName() + "$schema"));
            JCTree.JCExpression tableFieldAccess = treeMaker.Ident(names.fromString(pv.getFieldName() + "$table"));
            JCTree.JCExpression columnFieldAccess = treeMaker.Ident(names.fromString(pv.getFieldName() + "$column"));

            JCTree.JCExpression fieldAccess = treeMaker.Select(
                    treeMaker.Ident(names.fromString("this")),
                    names.fromString(pv.getFieldName())
            );

            String insertStrategy = null;
            if (pv instanceof ParsedForeignPersistentValue foreignPv) {
                insertStrategy = foreignPv.getInsertStrategy();
            }

            JCTree.JCExpression insertStatement = treeMaker.Apply(
                    List.nil(),
                    treeMaker.Select(
                            treeMaker.Ident(names.fromString("ctx")),
                            names.fromString("set")
                    ),
                    List.of(
                            schemaFieldAccess,
                            tableFieldAccess,
                            columnFieldAccess,
                            fieldAccess,
                            insertStrategy != null ?
                                    treeMaker.Select(
                                            treeMaker.Ident(names.fromString("InsertStrategy")),
                                            names.fromString(insertStrategy)
                                    )
                                    :
                                    treeMaker.Literal(TypeTag.BOT, null)
                    )
            );

            bodyStatements.add(treeMaker.Exec(insertStatement));
        }

        for (ParsedReference ref : parsedReferences) {
            String idColumnValuePairsFieldName = ref.getFieldName() + "_reference$idColumnValuePairs";
            String schemaFieldName = ref.getFieldName() + "_reference$schema";
            String tableFieldName = ref.getFieldName() + "_reference$table";


            bodyStatements.add(
                    treeMaker.If(
                            treeMaker.Binary(
                                    JCTree.Tag.NE,
                                    treeMaker.Ident(names.fromString(idColumnValuePairsFieldName)),
                                    treeMaker.Literal(TypeTag.BOT, null)
                            ),
                            treeMaker.ForLoop(
                                    List.of(
                                            treeMaker.VarDef(
                                                    treeMaker.Modifiers(0),
                                                    names.fromString("i"),
                                                    treeMaker.TypeIdent(TypeTag.INT),
                                                    treeMaker.Literal(0)
                                            )
                                    ),
                                    treeMaker.Binary(
                                            JCTree.Tag.LT,
                                            treeMaker.Ident(names.fromString("i")),
                                            treeMaker.Select(
                                                    treeMaker.Ident(names.fromString(idColumnValuePairsFieldName)),
                                                    names.fromString("length")
                                            )
                                    ),
                                    List.of(
                                            treeMaker.Exec(
                                                    treeMaker.Unary(
                                                            JCTree.Tag.POSTINC,
                                                            treeMaker.Ident(names.fromString("i"))
                                                    )
                                            )
                                    ),
                                    treeMaker.Block(
                                            0,
                                            List.of(
                                                    treeMaker.Exec(
                                                            treeMaker.Apply(
                                                                    List.nil(),
                                                                    treeMaker.Select(
                                                                            treeMaker.Ident(names.fromString("ctx")),
                                                                            names.fromString("set")
                                                                    ),
                                                                    List.of(
                                                                            treeMaker.Select(
                                                                                    treeMaker.Ident(names.fromString("this")),
                                                                                    names.fromString(schemaFieldName)
                                                                            ),
                                                                            treeMaker.Select(
                                                                                    treeMaker.Ident(names.fromString("this")),
                                                                                    names.fromString(tableFieldName)
                                                                            ),
                                                                            treeMaker.Apply(
                                                                                    List.nil(),
                                                                                    treeMaker.Select(
                                                                                            treeMaker.Indexed(
                                                                                                    treeMaker.Ident(names.fromString(idColumnValuePairsFieldName)),
                                                                                                    treeMaker.Ident(names.fromString("i"))
                                                                                            ),
                                                                                            names.fromString("column")
                                                                                    ),
                                                                                    List.nil()
                                                                            ),
                                                                            treeMaker.Apply(
                                                                                    List.nil(),
                                                                                    treeMaker.Select(
                                                                                            treeMaker.Indexed(
                                                                                                    treeMaker.Ident(names.fromString(idColumnValuePairsFieldName)),
                                                                                                    treeMaker.Ident(names.fromString("i"))
                                                                                            ),
                                                                                            names.fromString("value")
                                                                                    ),
                                                                                    List.nil()
                                                                            ),
                                                                            treeMaker.Select(
                                                                                    treeMaker.Ident(names.fromString("InsertStrategy")),
                                                                                    names.fromString("OVERWRITE_EXISTING")
                                                                            )
                                                                    )
                                                            )
                                                    )

                                            )
                                    )
                            ),
                            null
                    )
            );
        }

        JCTree.JCMethodDecl insertMethod = treeMaker.MethodDef(
                treeMaker.Modifiers(Flags.PUBLIC | Flags.FINAL),
                names.fromString("insert"),
                treeMaker.TypeIdent(TypeTag.VOID),
                List.nil(),
                List.of(
                        treeMaker.VarDef(
                                treeMaker.Modifiers(Flags.PARAMETER),
                                names.fromString("ctx"),
                                treeMaker.Ident(names.fromString("InsertContext")),
                                null
                        )
                ),
                List.nil(),
                treeMaker.Block(0, List.from(bodyStatements)),
                null
        );
        builderClassDecl.defs = builderClassDecl.defs.append(insertMethod);
    }

    public void makeInsertModeMethod() {
        JCTree.JCMethodDecl insertMethod = treeMaker.MethodDef(
                treeMaker.Modifiers(Flags.PUBLIC | Flags.FINAL),
                names.fromString("insert"),
                treeMaker.Ident(dataClassDecl.name),
                List.nil(),
                List.of(
                        treeMaker.VarDef(
                                treeMaker.Modifiers(Flags.PARAMETER),
                                names.fromString("mode"),
                                treeMaker.Ident(names.fromString("InsertMode")),
                                null
                        )
                ),
                List.nil(),
                treeMaker.Block(0, List.of(
                        treeMaker.VarDef(
                                treeMaker.Modifiers(0),
                                names.fromString("ctx"),
                                treeMaker.Ident(names.fromString("InsertContext")),
                                treeMaker.Apply(
                                        List.nil(),
                                        treeMaker.Select(
                                                treeMaker.Ident(names.fromString("dataManager")),
                                                names.fromString("createInsertContext")
                                        ),
                                        List.nil()
                                )
                        ),
                        treeMaker.Exec(
                                treeMaker.Apply(
                                        List.nil(),
                                        treeMaker.Select(
                                                treeMaker.Ident(names.fromString("this")),
                                                names.fromString("insert")
                                        ),
                                        List.of(
                                                treeMaker.Ident(names.fromString("ctx"))
                                        )
                                )
                        ),
                        treeMaker.Return(
                                treeMaker.Apply(
                                        List.nil(),
                                        treeMaker.Select(
                                                treeMaker.Apply(
                                                        List.nil(),
                                                        treeMaker.Select(
                                                                treeMaker.Ident(names.fromString("ctx")),
                                                                names.fromString("insert")
                                                        ),
                                                        List.of(
                                                                treeMaker.Ident(names.fromString("mode"))
                                                        )
                                                ),
                                                names.fromString("get")
                                        ),
                                        List.of(
                                                treeMaker.Select(
                                                        treeMaker.Ident(dataClassDecl.name),
                                                        names.fromString("class")
                                                )
                                        )
                                )
                        )
                )),
                null
        );
        builderClassDecl.defs = builderClassDecl.defs.append(insertMethod);
    }
}
