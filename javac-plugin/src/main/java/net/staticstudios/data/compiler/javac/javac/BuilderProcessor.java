package net.staticstudios.data.compiler.javac.javac;

import com.sun.tools.javac.code.Flags;
import com.sun.tools.javac.code.TypeTag;
import com.sun.tools.javac.tree.JCTree;
import com.sun.tools.javac.util.List;
import net.staticstudios.data.InsertStrategy;
import net.staticstudios.data.compiler.javac.ProcessorContext;

import java.util.ArrayList;
import java.util.Collection;

public class BuilderProcessor extends AbstractBuilderProcessor {
    private final Collection<ParsedPersistentValue> persistentValues;
    private final Collection<ParsedReference> references;

    public BuilderProcessor(ProcessorContext processorContext) {
        super(processorContext, "Builder", "builder");
        this.persistentValues = processorContext.persistentValues();
        this.references = processorContext.references();
    }

    public static boolean hasProcessed(JCTree.JCClassDecl classDecl) {
        return classDecl.defs.stream()
                .anyMatch(def -> def instanceof JCTree.JCClassDecl &&
                        ((JCTree.JCClassDecl) def).name.toString().equals(classDecl.name + "Builder"));
    }

    @Override
    protected void process() {
        for (ParsedPersistentValue pv : persistentValues) {
            processValue(pv);
        }

        for (ParsedReference ref : references) {
            processReference(ref);
        }

        makeInsertContextMethod(persistentValues, references);
        makeInsertModeMethod();
    }


    private void processValue(ParsedPersistentValue pv) {
        storeSchema(pv.getFieldName(), pv.getSchema());
        storeTable(pv.getFieldName(), pv.getTable());
        storeColumn(pv.getFieldName(), pv.getColumn());

        createField(VarDef(
                Modifiers(Flags.PRIVATE),
                names.fromString(pv.getFieldName()),
                chainDots(pv.getTypeFQNParts()),
                Literal(TypeTag.BOT, null)
        ), builderClassDecl);

        createMethod(MethodDef(
                Modifiers(Flags.PUBLIC | Flags.FINAL),
                names.fromString(pv.getFieldName()),
                Ident(names.fromString(getBuilderClassName())),
                List.nil(),
                List.of(
                        VarDef(
                                Modifiers(Flags.PARAMETER),
                                names.fromString(pv.getFieldName()),
                                chainDots(pv.getTypeFQNParts()),
                                null
                        )
                ),
                List.nil(),
                Block(0, List.of(
                        Exec(
                                Assign(
                                        Select(
                                                Ident(names.fromString("this")),
                                                names.fromString(pv.getFieldName())
                                        ),
                                        Ident(names.fromString(pv.getFieldName()))
                                )
                        ),
                        Return(
                                Ident(names.fromString("this"))
                        )
                )),
                null
        ), builderClassDecl);
    }

    private void processReference(ParsedReference ref) {
        String idColumnValuePairsFieldName = ref.getFieldName() + "_reference$idColumnValuePairs";
        String schemaFieldName = ref.getFieldName() + "_reference$schema";
        String tableFieldName = ref.getFieldName() + "_reference$table";

        createField(VarDef(
                Modifiers(Flags.PRIVATE),
                names.fromString(idColumnValuePairsFieldName),
                TypeArray(chainDots("net", "staticstudios", "data", "util", "ColumnValuePair")),
                Literal(TypeTag.BOT, null)
        ), builderClassDecl);

        createField(VarDef(
                Modifiers(Flags.PRIVATE),
                names.fromString(schemaFieldName),
                Ident(names.fromString("String")),
                Literal(TypeTag.BOT, null)
        ), builderClassDecl);

        createField(VarDef(
                Modifiers(Flags.PRIVATE),
                names.fromString(tableFieldName),
                Ident(names.fromString("String")),
                Literal(TypeTag.BOT, null)
        ), builderClassDecl);

        var handleNotNull = Block(0, List.of(
                Exec(
                        Assign(
                                Select(
                                        Ident(names.fromString("this")),
                                        names.fromString(idColumnValuePairsFieldName)
                                ),
                                Apply(
                                        List.nil(),
                                        Select(
                                                Apply(
                                                        List.nil(),
                                                        Select(
                                                                Ident(names.fromString(ref.getFieldName())),
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
                VarDef(
                        Modifiers(0),
                        names.fromString("__$metadata"),
                        chainDots("net", "staticstudios", "data", "util", "UniqueDataMetadata"),
                        Apply(
                                List.nil(),
                                Select(
                                        Ident(names.fromString(ref.getFieldName())),
                                        names.fromString("getMetadata")
                                ),
                                List.nil()
                        )
                ),
                Exec(
                        Assign(
                                Select(
                                        Ident(names.fromString("this")),
                                        names.fromString(schemaFieldName)
                                ),
                                Apply(
                                        List.nil(),
                                        Select(
                                                Ident(names.fromString("__$metadata")),
                                                names.fromString("schema")
                                        ),
                                        List.nil()
                                )
                        )
                ),
                Exec(
                        Assign(
                                Select(
                                        Ident(names.fromString("this")),
                                        names.fromString(tableFieldName)
                                ),
                                Apply(
                                        List.nil(),
                                        Select(
                                                Ident(names.fromString("__$metadata")),
                                                names.fromString("table")
                                        ),
                                        List.nil()
                                )
                        )
                )
        ));

        var handleNull = Block(0, List.of(
                Exec(
                        Assign(
                                Select(
                                        Ident(names.fromString("this")),
                                        names.fromString(idColumnValuePairsFieldName)
                                ),
                                Literal(TypeTag.BOT, null)
                        )
                ),
                Exec(
                        Assign(
                                Select(
                                        Ident(names.fromString("this")),
                                        names.fromString(schemaFieldName)
                                ),
                                Literal(TypeTag.BOT, null)
                        )
                ),
                Exec(
                        Assign(
                                Select(
                                        Ident(names.fromString("this")),
                                        names.fromString(tableFieldName)
                                ),
                                Literal(TypeTag.BOT, null)
                        )
                )
        ));

        createMethod(MethodDef(
                Modifiers(Flags.PUBLIC | Flags.FINAL),
                names.fromString(ref.getFieldName()),
                Ident(names.fromString(getBuilderClassName())),
                List.nil(),
                List.of(
                        VarDef(
                                Modifiers(Flags.PARAMETER),
                                names.fromString(ref.getFieldName()),
                                chainDots(ref.getTypeFQNParts()),
                                null
                        )
                ),
                List.nil(),
                Block(0, List.of(
                        If(
                                Binary(
                                        JCTree.Tag.NE,
                                        Ident(names.fromString(ref.getFieldName())),
                                        Literal(TypeTag.BOT, null)
                                ),
                                handleNotNull,
                                handleNull
                        ),
                        Return(
                                Ident(names.fromString("this"))
                        )
                )),
                null
        ), builderClassDecl);
    }

    private void makeInsertContextMethod(Collection<ParsedPersistentValue> parsedPersistentValues, Collection<ParsedReference> parsedReferences) {
        java.util.List<JCTree.JCStatement> bodyStatements = new ArrayList<>();

        for (ParsedPersistentValue pv : parsedPersistentValues) {
            JCTree.JCExpression schemaFieldAccess = Ident(names.fromString(pv.getFieldName() + "$schema"));
            JCTree.JCExpression tableFieldAccess = Ident(names.fromString(pv.getFieldName() + "$table"));
            JCTree.JCExpression columnFieldAccess = Ident(names.fromString(pv.getFieldName() + "$column"));

            JCTree.JCExpression fieldAccess = Select(
                    Ident(names.fromString("this")),
                    names.fromString(pv.getFieldName())
            );

            InsertStrategy insertStrategy = null;
            if (pv instanceof ParsedForeignPersistentValue foreignPv) {
                insertStrategy = foreignPv.getInsertStrategy();
            }

            JCTree.JCExpression insertStatement = Apply(
                    List.nil(),
                    Select(
                            Ident(names.fromString("ctx")),
                            names.fromString("set")
                    ),
                    List.of(
                            schemaFieldAccess,
                            tableFieldAccess,
                            columnFieldAccess,
                            fieldAccess,
                            insertStrategy != null ?
                                    Select(
                                            chainDots("net", "staticstudios", "data", "InsertStrategy"),
                                            names.fromString(insertStrategy.name())
                                    )
                                    :
                                    Literal(TypeTag.BOT, null)
                    )
            );

            bodyStatements.add(Exec(insertStatement));
        }

        for (ParsedReference ref : parsedReferences) {
            String idColumnValuePairsFieldName = ref.getFieldName() + "_reference$idColumnValuePairs";
            String schemaFieldName = ref.getFieldName() + "_reference$schema";
            String tableFieldName = ref.getFieldName() + "_reference$table";


            bodyStatements.add(
                    If(
                            Binary(
                                    JCTree.Tag.NE,
                                    Ident(names.fromString(idColumnValuePairsFieldName)),
                                    Literal(TypeTag.BOT, null)
                            ),
                            ForLoop(
                                    List.of(
                                            VarDef(
                                                    Modifiers(0),
                                                    names.fromString("i"),
                                                    TypeIdent(TypeTag.INT),
                                                    Literal(0)
                                            )
                                    ),
                                    Binary(
                                            JCTree.Tag.LT,
                                            Ident(names.fromString("i")),
                                            Select(
                                                    Ident(names.fromString(idColumnValuePairsFieldName)),
                                                    names.fromString("length")
                                            )
                                    ),
                                    List.of(
                                            Exec(
                                                    Unary(
                                                            JCTree.Tag.POSTINC,
                                                            Ident(names.fromString("i"))
                                                    )
                                            )
                                    ),
                                    Block(
                                            0,
                                            List.of(
                                                    Exec(
                                                            Apply(
                                                                    List.nil(),
                                                                    Select(
                                                                            Ident(names.fromString("ctx")),
                                                                            names.fromString("set")
                                                                    ),
                                                                    List.of(
                                                                            Select(
                                                                                    Ident(names.fromString("this")),
                                                                                    names.fromString(schemaFieldName)
                                                                            ),
                                                                            Select(
                                                                                    Ident(names.fromString("this")),
                                                                                    names.fromString(tableFieldName)
                                                                            ),
                                                                            Apply(
                                                                                    List.nil(),
                                                                                    Select(
                                                                                            Indexed(
                                                                                                    Ident(names.fromString(idColumnValuePairsFieldName)),
                                                                                                    Ident(names.fromString("i"))
                                                                                            ),
                                                                                            names.fromString("column")
                                                                                    ),
                                                                                    List.nil()
                                                                            ),
                                                                            Apply(
                                                                                    List.nil(),
                                                                                    Select(
                                                                                            Indexed(
                                                                                                    Ident(names.fromString(idColumnValuePairsFieldName)),
                                                                                                    Ident(names.fromString("i"))
                                                                                            ),
                                                                                            names.fromString("value")
                                                                                    ),
                                                                                    List.nil()
                                                                            ),
                                                                            Select(
                                                                                    chainDots("net", "staticstudios", "data", "InsertStrategy"),
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

        createMethod(MethodDef(
                Modifiers(Flags.PUBLIC | Flags.FINAL),
                names.fromString("insert"),
                TypeIdent(TypeTag.VOID),
                List.nil(),
                List.of(
                        VarDef(
                                Modifiers(Flags.PARAMETER),
                                names.fromString("ctx"),
                                chainDots("net", "staticstudios", "data", "insert", "InsertContext"),
                                null
                        )
                ),
                List.nil(),
                Block(0, List.from(bodyStatements)),
                null
        ), builderClassDecl);

    }

    public void makeInsertModeMethod() {
        createMethod(MethodDef(
                Modifiers(Flags.PUBLIC | Flags.FINAL),
                names.fromString("insert"),
                Ident(dataClassDecl.name),
                List.nil(),
                List.of(
                        VarDef(
                                Modifiers(Flags.PARAMETER),
                                names.fromString("mode"),
                                chainDots("net", "staticstudios", "data", "InsertMode"),
                                null
                        )
                ),
                List.nil(),
                Block(0, List.of(
                        VarDef(
                                Modifiers(0),
                                names.fromString("ctx"),
                                chainDots("net", "staticstudios", "data", "insert", "InsertContext"),
                                Apply(
                                        List.nil(),
                                        Select(
                                                Ident(names.fromString("dataManager")),
                                                names.fromString("createInsertContext")
                                        ),
                                        List.nil()
                                )
                        ),
                        Exec(
                                Apply(
                                        List.nil(),
                                        Select(
                                                Ident(names.fromString("this")),
                                                names.fromString("insert")
                                        ),
                                        List.of(
                                                Ident(names.fromString("ctx"))
                                        )
                                )
                        ),
                        Return(
                                Apply(
                                        List.nil(),
                                        Select(
                                                Apply(
                                                        List.nil(),
                                                        Select(
                                                                Ident(names.fromString("ctx")),
                                                                names.fromString("insert")
                                                        ),
                                                        List.of(
                                                                Ident(names.fromString("mode"))
                                                        )
                                                ),
                                                names.fromString("get")
                                        ),
                                        List.of(
                                                Select(
                                                        Ident(dataClassDecl.name),
                                                        names.fromString("class")
                                                )
                                        )
                                )
                        )
                )),
                null
        ), builderClassDecl);

    }
}
