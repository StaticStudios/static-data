package net.staticstudios.data.compiler.javac.javac;

import com.sun.tools.javac.code.Flags;
import com.sun.tools.javac.code.TypeTag;
import com.sun.tools.javac.tree.JCTree;
import com.sun.tools.javac.util.List;
import net.staticstudios.data.compiler.javac.ProcessorContext;
import net.staticstudios.data.utils.StringUtils;
import org.jetbrains.annotations.Nullable;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

public class QueryBuilderProcessor extends AbstractBuilderProcessor {
    private final Collection<ParsedPersistentValue> persistentValues;
    private final Collection<ParsedReference> references;
    private final String whereClassName;

    public QueryBuilderProcessor(ProcessorContext processorContext) {
        super(processorContext, "QueryBuilder", "query");
        this.persistentValues = processorContext.persistentValues();
        this.references = processorContext.references();

        QueryWhereProcessor whereProcessor = new QueryWhereProcessor(processorContext);
        this.whereClassName = whereProcessor.getBuilderClassName();
        whereProcessor.runProcessor();
    }

    @Override
    protected @Nullable SuperClass extending() {
        return new SuperClass(
                "net.staticstudios.data.query.BaseQueryBuilder",
                List.of(
                        Ident(dataClassDecl.name),
                        Ident(names.fromString(whereClassName))
                ),
                List.of(
                        Ident(names.fromString("dataManager")),
                        Select(
                                Ident(dataClassDecl.name),
                                names.fromString("class")
                        ),
                        NewClass(
                                null,
                                List.nil(),
                                Ident(names.fromString(whereClassName)),
                                List.nil(),
                                null
                        )
                )
        );
    }

    @Override
    protected void process() {
        addWhereMethod();
        addLimitMethod();
        addOffsetMethod();

        for (ParsedPersistentValue pv : persistentValues) {
            processValue(pv);
        }
    }


    private void processValue(ParsedPersistentValue pv) {
        String schemaFieldName = storeSchema(pv.getFieldName(), pv.getSchema());
        String tableFieldName = storeTable(pv.getFieldName(), pv.getTable());
        String columnFieldName = storeColumn(pv.getFieldName(), pv.getColumn());

        addOrderByMethod(schemaFieldName, tableFieldName, columnFieldName, pv.getFieldName());
    }

    private void addOrderByMethod(String schemaFieldName, String tableFieldName, String columnFieldName, String fieldName) {
        createMethod(MethodDef(
                Modifiers(Flags.PUBLIC | Flags.FINAL),
                names.fromString("orderBy" + StringUtils.capitalize(fieldName)),
                Ident(names.fromString(getBuilderClassName())),
                List.nil(),
                List.of(
                        VarDef(
                                Modifiers(Flags.PARAMETER),
                                names.fromString("order"),
                                chainDots("net", "staticstudios", "data", "Order"),
                                null
                        )
                ),
                List.nil(),
                Block(0, List.of(
                        Exec(
                                Apply(
                                        List.nil(),
                                        Select(
                                                Ident(names.fromString("super")),
                                                names.fromString("setOrderBy")
                                        ),
                                        List.of(
                                                Ident(names.fromString(schemaFieldName)),
                                                Ident(names.fromString(tableFieldName)),
                                                Ident(names.fromString(columnFieldName)),
                                                Ident(names.fromString("order"))
                                        )
                                )
                        ),
                        Return(
                                Ident(names.fromString("this"))
                        )
                )),
                null
        ), builderClassDecl);

    }

    private void addLimitMethod() {
        createMethod(MethodDef(
                Modifiers(Flags.PUBLIC | Flags.FINAL),
                names.fromString("limit"),
                Ident(names.fromString(getBuilderClassName())),
                List.nil(),
                List.of(
                        VarDef(
                                Modifiers(Flags.PARAMETER),
                                names.fromString("limit"),
                                TypeIdent(TypeTag.INT),
                                null
                        )
                ),
                List.nil(),
                Block(0, List.of(
                        Exec(
                                Apply(
                                        List.nil(),
                                        Select(
                                                Ident(names.fromString("super")),
                                                names.fromString("setLimit")
                                        ),
                                        List.of(
                                                Ident(names.fromString("limit"))
                                        )
                                )
                        ),
                        Return(
                                Ident(names.fromString("this"))
                        )
                )),
                null
        ), builderClassDecl);

    }

    private void addOffsetMethod() {
        createMethod(MethodDef(
                Modifiers(Flags.PUBLIC | Flags.FINAL),
                names.fromString("offset"),
                Ident(names.fromString(getBuilderClassName())),
                List.nil(),
                List.of(
                        VarDef(
                                Modifiers(Flags.PARAMETER),
                                names.fromString("offset"),
                                TypeIdent(TypeTag.INT),
                                null
                        )
                ),
                List.nil(),
                Block(0, List.of(
                        Exec(
                                Apply(
                                        List.nil(),
                                        Select(
                                                Ident(names.fromString("super")),
                                                names.fromString("setOffset")
                                        ),
                                        List.of(
                                                Ident(names.fromString("offset"))
                                        )
                                )
                        ),
                        Return(
                                Ident(names.fromString("this"))
                        )
                )),
                null
        ), builderClassDecl);

    }

    private void addWhereMethod() {
        createMethod(MethodDef(
                Modifiers(Flags.PUBLIC | Flags.FINAL),
                names.fromString("where"),
                Ident(names.fromString(getBuilderClassName())),
                List.nil(),
                List.of(
                        VarDef(
                                Modifiers(Flags.PARAMETER),
                                names.fromString("function"),
                                TypeApply(
                                        chainDots("java", "util", "function", "Function"),
                                        List.of(
                                                Ident(names.fromString(whereClassName)),
                                                Ident(names.fromString(whereClassName))
                                        )
                                ),
                                null
                        )
                ),
                List.nil(),
                Block(0, List.of(
                        Exec(
                                Apply(
                                        List.nil(),
                                        Select(
                                                Ident(names.fromString("function")),
                                                names.fromString("apply")
                                        ),
                                        List.of(
                                                Select(
                                                        Ident(names.fromString("super")),
                                                        names.fromString("where")
                                                )
                                        )
                                )
                        ),
                        Return(
                                Ident(names.fromString("this"))
                        )
                )),
                null
        ), builderClassDecl);

    }

    class QueryWhereProcessor extends AbstractBuilderProcessor {
        private String dataSchemaFieldName;
        private String dataTableFieldName;

        public QueryWhereProcessor(ProcessorContext processorContext) {
            super(processorContext, "QueryWhere", null);
        }

        @Override
        protected @Nullable SuperClass extending() {
            return new SuperClass(
                    "net.staticstudios.data.query.BaseQueryWhere",
                    List.nil(),
                    List.nil()
            );
        }

        @Override
        protected void process() {
            dataSchemaFieldName = storeSchema("data", dataAnnotation.schema());
            dataTableFieldName = storeTable("data", dataAnnotation.table());

            addGroupMethod();
            addAndMethod();
            addOrMethod();

            for (ParsedPersistentValue pv : persistentValues) {
                processValue(pv);
            }

//            for (ParsedReference ref : references) {
            //todo: process references and support is, isNot, isNull and isNotNull
//            }
        }

        private void processValue(ParsedPersistentValue pv) {
            String schemaFieldName = storeSchema(pv.getFieldName(), pv.getSchema());
            String tableFieldName = storeTable(pv.getFieldName(), pv.getTable());
            String columnFieldName = storeColumn(pv.getFieldName(), pv.getColumn());

            ParsedForeignPersistentValue fpv = null;
            if (pv instanceof ParsedForeignPersistentValue _fpv) {
                fpv = _fpv;
                storeLinks(fpv.getFieldName(), fpv.getLinks());
            }

            addIsMethod(pv, schemaFieldName, tableFieldName, columnFieldName);
            addIsNotMethod(pv, schemaFieldName, tableFieldName, columnFieldName);

            addIsInCollectionMethod(pv, schemaFieldName, tableFieldName, columnFieldName);
            addIsInArrayMethod(pv, schemaFieldName, tableFieldName, columnFieldName);
            addIsNotInCollectionMethod(pv, schemaFieldName, tableFieldName, columnFieldName);
            addIsNotInArrayMethod(pv, schemaFieldName, tableFieldName, columnFieldName);

            if (pv.isNullable()) {
                addIsNullMethod(pv, schemaFieldName, tableFieldName, columnFieldName);
                addIsNotNullMethod(pv, schemaFieldName, tableFieldName, columnFieldName);
            }

            if (typeUtils.isType(pv.getType(), String.class)) {
                addIsLikeMethod(pv, schemaFieldName, tableFieldName, columnFieldName);
                addIsNotLikeMethod(pv, schemaFieldName, tableFieldName, columnFieldName);
            }

            if (typeUtils.isNumericType(pv.getType()) || typeUtils.isType(pv.getType(), Timestamp.class)) {
                addIsLessThanMethod(pv, schemaFieldName, tableFieldName, columnFieldName);
                addIsLessThanOrEqualToMethod(pv, schemaFieldName, tableFieldName, columnFieldName);
                addIsGreaterThanMethod(pv, schemaFieldName, tableFieldName, columnFieldName);
                addIsGreaterThanOrEqualToMethod(pv, schemaFieldName, tableFieldName, columnFieldName);
                addIsBetweenMethod(pv, schemaFieldName, tableFieldName, columnFieldName);
                addIsNotBetweenMethod(pv, schemaFieldName, tableFieldName, columnFieldName);
            }
        }

        private List<JCTree.JCStatement> clause(ParsedPersistentValue pv, JCTree.JCStatement... statements) {
            java.util.List<JCTree.JCStatement> list = new ArrayList<>();

            if (pv instanceof ParsedForeignPersistentValue fpv) {
                String referencedSchemaFieldName = getStoredSchemaFieldName(fpv.getFieldName());
                String referencedTableFieldName = getStoredTableFieldName(fpv.getFieldName());
                String referencedColumnsFieldName = getStoredReferencedColumnsFieldName(fpv.getFieldName());
                String referringColumnsFieldName = getStoredReferringColumnsFieldName(fpv.getFieldName());
                list.add(
                        Exec(
                                Apply(
                                        List.nil(),
                                        Select(
                                                Ident(names.fromString("super")),
                                                names.fromString("addInnerJoin")
                                        ),
                                        List.of(
                                                Ident(names.fromString(dataSchemaFieldName)),
                                                Ident(names.fromString(dataTableFieldName)),
                                                Ident(names.fromString(referringColumnsFieldName)),
                                                Ident(names.fromString(referencedSchemaFieldName)),
                                                Ident(names.fromString(referencedTableFieldName)),
                                                Ident(names.fromString(referencedColumnsFieldName)
                                                )
                                        )
                                )
                        )
                );
            }

            list.addAll(Arrays.asList(statements));

            return List.from(list);
        }

        private void addIsMethod(ParsedPersistentValue pv, String schemaFieldName, String tableFieldName, String columnFieldName) {
            createMethod(MethodDef(
                    Modifiers(Flags.PUBLIC | Flags.FINAL),
                    names.fromString(pv.getFieldName() + "Is"),
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
                    Block(0, clause(pv,
                            Exec(
                                    Apply(
                                            List.nil(),
                                            Select(
                                                    Ident(names.fromString("super")),
                                                    names.fromString("equalsClause")
                                            ),
                                            List.of(
                                                    Ident(names.fromString(schemaFieldName)),
                                                    Ident(names.fromString(tableFieldName)),
                                                    Ident(names.fromString(columnFieldName)),
                                                    Ident(names.fromString(pv.getFieldName()))
                                            )
                                    )
                            ),
                            Return(
                                    Ident(names.fromString("this"))
                            )
                    )),
                    null
            ), builderClassDecl);
        }

        private void addIsNotMethod(ParsedPersistentValue pv, String schemaFieldName, String tableFieldName, String columnFieldName) {
            createMethod(MethodDef(
                    Modifiers(Flags.PUBLIC | Flags.FINAL),
                    names.fromString(pv.getFieldName() + "IsNot"),
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
                    Block(0, clause(pv,
                            Exec(
                                    Apply(
                                            List.nil(),
                                            Select(
                                                    Ident(names.fromString("super")),
                                                    names.fromString("equalsClause")
                                            ),
                                            List.of(
                                                    Ident(names.fromString(schemaFieldName)),
                                                    Ident(names.fromString(tableFieldName)),
                                                    Ident(names.fromString(columnFieldName)),
                                                    Ident(names.fromString(pv.getFieldName()))
                                            )
                                    )
                            ),
                            Return(
                                    Ident(names.fromString("this"))
                            )
                    )),
                    null
            ), builderClassDecl);
        }

        private void addIsNullMethod(ParsedPersistentValue pv, String schemaFieldName, String tableFieldName, String columnFieldName) {
            createMethod(MethodDef(
                    Modifiers(Flags.PUBLIC | Flags.FINAL),
                    names.fromString(pv.getFieldName() + "IsNull"),
                    Ident(names.fromString(getBuilderClassName())),
                    List.nil(),
                    List.nil(),
                    List.nil(),
                    Block(0, clause(pv,
                            Exec(
                                    Apply(
                                            List.nil(),
                                            Select(
                                                    Ident(names.fromString("super")),
                                                    names.fromString("nullClause")
                                            ),
                                            List.of(
                                                    Ident(names.fromString(schemaFieldName)),
                                                    Ident(names.fromString(tableFieldName)),
                                                    Ident(names.fromString(columnFieldName))
                                            )
                                    )
                            ),
                            Return(
                                    Ident(names.fromString("this"))
                            )
                    )),
                    null
            ), builderClassDecl);
        }

        private void addIsNotNullMethod(ParsedPersistentValue pv, String schemaFieldName, String tableFieldName, String columnFieldName) {
            createMethod(MethodDef(
                    Modifiers(Flags.PUBLIC | Flags.FINAL),
                    names.fromString(pv.getFieldName() + "IsNotNull"),
                    Ident(names.fromString(getBuilderClassName())),
                    List.nil(),
                    List.nil(),
                    List.nil(),
                    Block(0, clause(pv,
                            Exec(
                                    Apply(
                                            List.nil(),
                                            Select(
                                                    Ident(names.fromString("super")),
                                                    names.fromString("notNullClause")
                                            ),
                                            List.of(
                                                    Ident(names.fromString(schemaFieldName)),
                                                    Ident(names.fromString(tableFieldName)),
                                                    Ident(names.fromString(columnFieldName))
                                            )
                                    )
                            ),
                            Return(
                                    Ident(names.fromString("this"))
                            )
                    )),
                    null
            ), builderClassDecl);
        }

        private void addIsInCollectionMethod(ParsedPersistentValue pv, String schemaFieldName, String tableFieldName, String columnFieldName) {
            createMethod(MethodDef(
                    Modifiers(Flags.PUBLIC | Flags.FINAL),
                    names.fromString(pv.getFieldName() + "IsIn"),
                    Ident(names.fromString(getBuilderClassName())),
                    List.nil(),
                    List.of(
                            VarDef(
                                    Modifiers(Flags.PARAMETER),
                                    names.fromString(pv.getFieldName()),
                                    TypeApply(
                                            chainDots("java", "util", "Collection"),
                                            List.of(
                                                    chainDots(pv.getTypeFQNParts())
                                            )
                                    ),
                                    null
                            )
                    ),
                    List.nil(),
                    Block(0, clause(pv,
                            Exec(
                                    Apply(
                                            List.nil(),
                                            Select(
                                                    Ident(names.fromString("super")),
                                                    names.fromString("inClause")
                                            ),
                                            List.of(
                                                    Ident(names.fromString(schemaFieldName)),
                                                    Ident(names.fromString(tableFieldName)),
                                                    Ident(names.fromString(columnFieldName)),
                                                    Apply(
                                                            List.nil(),
                                                            Select(
                                                                    Ident(names.fromString(pv.getFieldName())),
                                                                    names.fromString("toArray")
                                                            ),
                                                            List.nil()
                                                    )
                                            )
                                    )
                            ),
                            Return(
                                    Ident(names.fromString("this"))
                            )
                    )),
                    null
            ), builderClassDecl);
        }

        private void addIsInArrayMethod(ParsedPersistentValue pv, String schemaFieldName, String tableFieldName, String columnFieldName) {
            createMethod(MethodDef(
                    Modifiers(Flags.PUBLIC | Flags.FINAL),
                    names.fromString(pv.getFieldName() + "IsIn"),
                    Ident(names.fromString(getBuilderClassName())),
                    List.nil(),
                    List.of(
                            VarDef(
                                    Modifiers(Flags.PARAMETER | Flags.VARARGS),
                                    names.fromString(pv.getFieldName()),
                                    TypeArray(
                                            chainDots(pv.getTypeFQNParts())
                                    ),
                                    null
                            )
                    ),
                    List.nil(),
                    Block(0, clause(pv,
                            Exec(
                                    Apply(
                                            List.nil(),
                                            Select(
                                                    Ident(names.fromString("super")),
                                                    names.fromString("inClause")
                                            ),
                                            List.of(
                                                    Ident(names.fromString(schemaFieldName)),
                                                    Ident(names.fromString(tableFieldName)),
                                                    Ident(names.fromString(columnFieldName)),
                                                    Ident(names.fromString(pv.getFieldName()))
                                            )
                                    )
                            ),
                            Return(
                                    Ident(names.fromString("this"))
                            )
                    )),
                    null
            ), builderClassDecl);
        }

        private void addIsNotInCollectionMethod(ParsedPersistentValue pv, String schemaFieldName, String tableFieldName, String columnFieldName) {
            createMethod(MethodDef(
                    Modifiers(Flags.PUBLIC | Flags.FINAL),
                    names.fromString(pv.getFieldName() + "IsNotIn"),
                    Ident(names.fromString(getBuilderClassName())),
                    List.nil(),
                    List.of(
                            VarDef(
                                    Modifiers(Flags.PARAMETER),
                                    names.fromString(pv.getFieldName()),
                                    TypeApply(
                                            chainDots("java", "util", "Collection"),
                                            List.of(
                                                    chainDots(pv.getTypeFQNParts())
                                            )
                                    ),
                                    null
                            )
                    ),
                    List.nil(),
                    Block(0, clause(pv,
                            Exec(
                                    Apply(
                                            List.nil(),
                                            Select(
                                                    Ident(names.fromString("super")),
                                                    names.fromString("notInClause")
                                            ),
                                            List.of(
                                                    Ident(names.fromString(schemaFieldName)),
                                                    Ident(names.fromString(tableFieldName)),
                                                    Ident(names.fromString(columnFieldName)),
                                                    Apply(
                                                            List.nil(),
                                                            Select(
                                                                    Ident(names.fromString(pv.getFieldName())),
                                                                    names.fromString("toArray")
                                                            ),
                                                            List.nil()
                                                    )
                                            )
                                    )
                            ),
                            Return(
                                    Ident(names.fromString("this"))
                            )
                    )),
                    null
            ), builderClassDecl);
        }

        private void addIsNotInArrayMethod(ParsedPersistentValue pv, String schemaFieldName, String tableFieldName, String columnFieldName) {
            createMethod(MethodDef(
                    Modifiers(Flags.PUBLIC | Flags.FINAL),
                    names.fromString(pv.getFieldName() + "IsNotIn"),
                    Ident(names.fromString(getBuilderClassName())),
                    List.nil(),
                    List.of(
                            VarDef(
                                    Modifiers(Flags.PARAMETER | Flags.VARARGS),
                                    names.fromString(pv.getFieldName()),
                                    TypeArray(
                                            chainDots(pv.getTypeFQNParts())
                                    ),
                                    null
                            )
                    ),
                    List.nil(),
                    Block(0, clause(pv,
                            Exec(
                                    Apply(
                                            List.nil(),
                                            Select(
                                                    Ident(names.fromString("super")),
                                                    names.fromString("notInClause")
                                            ),
                                            List.of(
                                                    Ident(names.fromString(schemaFieldName)),
                                                    Ident(names.fromString(tableFieldName)),
                                                    Ident(names.fromString(columnFieldName)),
                                                    Ident(names.fromString(pv.getFieldName()))
                                            )
                                    )
                            ),
                            Return(
                                    Ident(names.fromString("this"))
                            )
                    )),
                    null
            ), builderClassDecl);
        }

        private void addIsLikeMethod(ParsedPersistentValue pv, String schemaFieldName, String tableFieldName, String columnFieldName) {
            createMethod(MethodDef(
                    Modifiers(Flags.PUBLIC | Flags.FINAL),
                    names.fromString(pv.getFieldName() + "IsLike"),
                    Ident(names.fromString(getBuilderClassName())),
                    List.nil(),
                    List.of(
                            VarDef(
                                    Modifiers(Flags.PARAMETER),
                                    names.fromString("pattern"),
                                    Ident(names.fromString("String")),
                                    null
                            )
                    ),
                    List.nil(),
                    Block(0, clause(pv,
                            Exec(
                                    Apply(
                                            List.nil(),
                                            Select(
                                                    Ident(names.fromString("super")),
                                                    names.fromString("likeClause")
                                            ),
                                            List.of(
                                                    Ident(names.fromString(schemaFieldName)),
                                                    Ident(names.fromString(tableFieldName)),
                                                    Ident(names.fromString(columnFieldName)),
                                                    Ident(names.fromString("pattern"))
                                            )
                                    )
                            ),
                            Return(
                                    Ident(names.fromString("this"))
                            )
                    )),
                    null
            ), builderClassDecl);
        }

        private void addIsNotLikeMethod(ParsedPersistentValue pv, String schemaFieldName, String tableFieldName, String columnFieldName) {
            createMethod(MethodDef(
                    Modifiers(Flags.PUBLIC | Flags.FINAL),
                    names.fromString(pv.getFieldName() + "IsNotLike"),
                    Ident(names.fromString(getBuilderClassName())),
                    List.nil(),
                    List.of(
                            VarDef(
                                    Modifiers(Flags.PARAMETER),
                                    names.fromString("pattern"),
                                    Ident(names.fromString("String")),
                                    null
                            )
                    ),
                    List.nil(),
                    Block(0, clause(pv,
                            Exec(
                                    Apply(
                                            List.nil(),
                                            Select(
                                                    Ident(names.fromString("super")),
                                                    names.fromString("notLikeClause")
                                            ),
                                            List.of(
                                                    Ident(names.fromString(schemaFieldName)),
                                                    Ident(names.fromString(tableFieldName)),
                                                    Ident(names.fromString(columnFieldName)),
                                                    Ident(names.fromString("pattern"))
                                            )
                                    )
                            ),
                            Return(
                                    Ident(names.fromString("this"))
                            )
                    )),
                    null
            ), builderClassDecl);
        }

        private void addIsLessThanMethod(ParsedPersistentValue pv, String schemaFieldName, String tableFieldName, String columnFieldName) {
            createMethod(MethodDef(
                    Modifiers(Flags.PUBLIC | Flags.FINAL),
                    names.fromString(pv.getFieldName() + "IsLessThan"),
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
                    Block(0, clause(pv,
                            Exec(
                                    Apply(
                                            List.nil(),
                                            Select(
                                                    Ident(names.fromString("super")),
                                                    names.fromString("lessThanClause")
                                            ),
                                            List.of(
                                                    Ident(names.fromString(schemaFieldName)),
                                                    Ident(names.fromString(tableFieldName)),
                                                    Ident(names.fromString(columnFieldName)),
                                                    Ident(names.fromString(pv.getFieldName()))
                                            )
                                    )
                            ),
                            Return(
                                    Ident(names.fromString("this"))
                            )
                    )),
                    null
            ), builderClassDecl);
        }

        private void addIsLessThanOrEqualToMethod(ParsedPersistentValue pv, String schemaFieldName, String tableFieldName, String columnFieldName) {
            createMethod(MethodDef(
                    Modifiers(Flags.PUBLIC | Flags.FINAL),
                    names.fromString(pv.getFieldName() + "IsLessThanOrEqualTo"),
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
                    Block(0, clause(pv,
                            Exec(
                                    Apply(
                                            List.nil(),
                                            Select(
                                                    Ident(names.fromString("super")),
                                                    names.fromString("lessThanOrEqualToClause")
                                            ),
                                            List.of(
                                                    Ident(names.fromString(schemaFieldName)),
                                                    Ident(names.fromString(tableFieldName)),
                                                    Ident(names.fromString(columnFieldName)),
                                                    Ident(names.fromString(pv.getFieldName()))
                                            )
                                    )
                            ),
                            Return(
                                    Ident(names.fromString("this"))
                            )
                    )),
                    null
            ), builderClassDecl);
        }

        private void addIsGreaterThanMethod(ParsedPersistentValue pv, String schemaFieldName, String tableFieldName, String columnFieldName) {
            createMethod(MethodDef(
                    Modifiers(Flags.PUBLIC | Flags.FINAL),
                    names.fromString(pv.getFieldName() + "IsGreaterThan"),
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
                    Block(0, clause(pv,
                            Exec(
                                    Apply(
                                            List.nil(),
                                            Select(
                                                    Ident(names.fromString("super")),
                                                    names.fromString("greaterThanClause")
                                            ),
                                            List.of(
                                                    Ident(names.fromString(schemaFieldName)),
                                                    Ident(names.fromString(tableFieldName)),
                                                    Ident(names.fromString(columnFieldName)),
                                                    Ident(names.fromString(pv.getFieldName()))
                                            )
                                    )
                            ),
                            Return(
                                    Ident(names.fromString("this"))
                            )
                    )),
                    null
            ), builderClassDecl);
        }

        private void addIsGreaterThanOrEqualToMethod(ParsedPersistentValue pv, String schemaFieldName, String tableFieldName, String columnFieldName) {
            createMethod(MethodDef(
                    Modifiers(Flags.PUBLIC | Flags.FINAL),
                    names.fromString(pv.getFieldName() + "IsGreaterThanOrEqualTo"),
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
                    Block(0, clause(pv,
                            Exec(
                                    Apply(
                                            List.nil(),
                                            Select(
                                                    Ident(names.fromString("super")),
                                                    names.fromString("greaterThanOrEqualToClause")
                                            ),
                                            List.of(
                                                    Ident(names.fromString(schemaFieldName)),
                                                    Ident(names.fromString(tableFieldName)),
                                                    Ident(names.fromString(columnFieldName)),
                                                    Ident(names.fromString(pv.getFieldName()))
                                            )
                                    )
                            ),
                            Return(
                                    Ident(names.fromString("this"))
                            )
                    )),
                    null
            ), builderClassDecl);
        }

        private void addIsBetweenMethod(ParsedPersistentValue pv, String schemaFieldName, String tableFieldName, String columnFieldName) {
            createMethod(MethodDef(
                    Modifiers(Flags.PUBLIC | Flags.FINAL),
                    names.fromString(pv.getFieldName() + "IsBetween"),
                    Ident(names.fromString(getBuilderClassName())),
                    List.nil(),
                    List.of(
                            VarDef(
                                    Modifiers(Flags.PARAMETER),
                                    names.fromString("min"),
                                    chainDots(pv.getTypeFQNParts()),
                                    null
                            ),
                            VarDef(
                                    Modifiers(Flags.PARAMETER),
                                    names.fromString("max"),
                                    chainDots(pv.getTypeFQNParts()),
                                    null
                            )
                    ),
                    List.nil(),
                    Block(0, clause(pv,
                            Exec(
                                    Apply(
                                            List.nil(),
                                            Select(
                                                    Ident(names.fromString("super")),
                                                    names.fromString("betweenClause")
                                            ),
                                            List.of(
                                                    Ident(names.fromString(schemaFieldName)),
                                                    Ident(names.fromString(tableFieldName)),
                                                    Ident(names.fromString(columnFieldName)),
                                                    Ident(names.fromString("min")),
                                                    Ident(names.fromString("max"))
                                            )
                                    )
                            ),
                            Return(
                                    Ident(names.fromString("this"))
                            )
                    )),
                    null
            ), builderClassDecl);
        }

        private void addIsNotBetweenMethod(ParsedPersistentValue pv, String schemaFieldName, String tableFieldName, String columnFieldName) {
            createMethod(MethodDef(
                    Modifiers(Flags.PUBLIC | Flags.FINAL),
                    names.fromString(pv.getFieldName() + "IsNotBetween"),
                    Ident(names.fromString(getBuilderClassName())),
                    List.nil(),
                    List.of(
                            VarDef(
                                    Modifiers(Flags.PARAMETER),
                                    names.fromString("min"),
                                    chainDots(pv.getTypeFQNParts()),
                                    null
                            ),
                            VarDef(
                                    Modifiers(Flags.PARAMETER),
                                    names.fromString("max"),
                                    chainDots(pv.getTypeFQNParts()),
                                    null
                            )
                    ),
                    List.nil(),
                    Block(0, clause(pv,
                            Exec(
                                    Apply(
                                            List.nil(),
                                            Select(
                                                    Ident(names.fromString("super")),
                                                    names.fromString("notBetweenClause")
                                            ),
                                            List.of(
                                                    Ident(names.fromString(schemaFieldName)),
                                                    Ident(names.fromString(tableFieldName)),
                                                    Ident(names.fromString(columnFieldName)),
                                                    Ident(names.fromString("min")),
                                                    Ident(names.fromString("max"))
                                            )
                                    )
                            ),
                            Return(
                                    Ident(names.fromString("this"))
                            )
                    )),
                    null
            ), builderClassDecl);
        }

        private void addGroupMethod() {
            createMethod(MethodDef(
                    Modifiers(Flags.PUBLIC | Flags.FINAL),
                    names.fromString("group"),
                    Ident(names.fromString(getBuilderClassName())),
                    List.nil(),
                    List.of(
                            VarDef(
                                    Modifiers(Flags.PARAMETER),
                                    names.fromString("function"),
                                    TypeApply(
                                            chainDots("java", "util", "function", "Function"),
                                            List.of(
                                                    Ident(names.fromString(getBuilderClassName())),
                                                    Ident(names.fromString(getBuilderClassName()))
                                            )
                                    ),
                                    null
                            )
                    ),
                    List.nil(),
                    Block(0, List.of(
                            Exec(
                                    Apply(
                                            List.nil(),
                                            Select(
                                                    Ident(names.fromString("super")),
                                                    names.fromString("pushGroup")
                                            ),
                                            List.nil()
                                    )
                            ),
                            Exec(
                                    Apply(
                                            List.nil(),
                                            Select(
                                                    Ident(names.fromString("function")),
                                                    names.fromString("apply")
                                            ),
                                            List.of(
                                                    Ident(names.fromString("this"))
                                            )
                                    )
                            ),
                            Exec(
                                    Apply(
                                            List.nil(),
                                            Select(
                                                    Ident(names.fromString("super")),
                                                    names.fromString("popGroup")
                                            ),
                                            List.nil()
                                    )
                            ),
                            Return(
                                    Ident(names.fromString("this"))
                            )
                    )),
                    null
            ), builderClassDecl);
        }

        private void addAndMethod() {
            createMethod(MethodDef(
                    Modifiers(Flags.PUBLIC | Flags.FINAL),
                    names.fromString("and"),
                    Ident(names.fromString(getBuilderClassName())),
                    List.nil(),
                    List.nil(),
                    List.nil(),
                    Block(0, List.of(
                            Exec(
                                    Apply(
                                            List.nil(),
                                            Select(
                                                    Ident(names.fromString("super")),
                                                    names.fromString("andClause")
                                            ),
                                            List.nil()
                                    )
                            ),
                            Return(
                                    Ident(names.fromString("this"))
                            )
                    )),
                    null
            ), builderClassDecl);
        }

        private void addOrMethod() {
            createMethod(MethodDef(
                    Modifiers(Flags.PUBLIC | Flags.FINAL),
                    names.fromString("or"),
                    Ident(names.fromString(getBuilderClassName())),
                    List.nil(),
                    List.nil(),
                    List.nil(),
                    Block(0, List.of(
                            Exec(
                                    Apply(
                                            List.nil(),
                                            Select(
                                                    Ident(names.fromString("super")),
                                                    names.fromString("orClause")
                                            ),
                                            List.nil()
                                    )
                            ),
                            Return(
                                    Ident(names.fromString("this"))
                            )
                    )),
                    null
            ), builderClassDecl);
        }
    }
}
