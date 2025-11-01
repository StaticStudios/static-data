package net.staticstudios.data.compiler.javac;

import com.sun.tools.javac.code.Flags;
import com.sun.tools.javac.code.TypeTag;
import com.sun.tools.javac.tree.JCTree;
import com.sun.tools.javac.tree.TreeMaker;
import com.sun.tools.javac.util.List;
import com.sun.tools.javac.util.Names;
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

    public QueryBuilderProcessor(JCTree.JCCompilationUnit compilationUnit, TreeMaker treeMaker, Names names, JCTree.JCClassDecl dataClassDecl, ParsedDataAnnotation dataAnnotation,
                                 Collection<ParsedPersistentValue> persistentValues, Collection<ParsedReference> references
    ) {
        super(compilationUnit, treeMaker, names, dataClassDecl, dataAnnotation, "QueryBuilder", "query");
        this.persistentValues = persistentValues;
        this.references = references;

        QueryWhereProcessor whereProcessor = new QueryWhereProcessor(compilationUnit, treeMaker, names, dataClassDecl, dataAnnotation);
        this.whereClassName = whereProcessor.getBuilderClassName();
        whereProcessor.runProcessor();
    }

    @Override
    protected void addImports() {
        JavaCPluginUtils.importClass(compilationUnit, treeMaker, names, "net.staticstudios.data.util", "ValueUtils");
        JavaCPluginUtils.importClass(compilationUnit, treeMaker, names, "net.staticstudios.data.query", "BaseQueryBuilder");
        JavaCPluginUtils.importClass(compilationUnit, treeMaker, names, "net.staticstudios.data", "Order");
        JavaCPluginUtils.importClass(compilationUnit, treeMaker, names, "net.staticstudios.data.query", "BaseQueryWhere");
        JavaCPluginUtils.importClass(compilationUnit, treeMaker, names, "java.util.function", "Function");
        JavaCPluginUtils.importClass(compilationUnit, treeMaker, names, "java.util", "Collection");
    }

    @Override
    protected @Nullable SuperClass extending() {
        return new SuperClass(
                "BaseQueryBuilder",
                List.of(
                        treeMaker.Ident(dataClassDecl.name),
                        treeMaker.Ident(names.fromString(whereClassName))
                ),
                List.of(
                        treeMaker.Ident(names.fromString("dataManager")),
                        treeMaker.Select(
                                treeMaker.Ident(dataClassDecl.name),
                                names.fromString("class")
                        ),
                        treeMaker.NewClass(
                                null,
                                List.nil(),
                                treeMaker.Ident(names.fromString(whereClassName)),
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
        JCTree.JCMethodDecl orderByMethod = treeMaker.MethodDef(
                treeMaker.Modifiers(Flags.PUBLIC | Flags.FINAL),
                names.fromString("orderBy" + StringUtils.capitalize(fieldName)),
                treeMaker.Ident(names.fromString(getBuilderClassName())),
                List.nil(),
                List.of(
                        treeMaker.VarDef(
                                treeMaker.Modifiers(Flags.PARAMETER),
                                names.fromString("order"),
                                treeMaker.Ident(names.fromString("Order")),
                                null
                        )
                ),
                List.nil(),
                treeMaker.Block(0, List.of(
                        treeMaker.Exec(
                                treeMaker.Apply(
                                        List.nil(),
                                        treeMaker.Select(
                                                treeMaker.Ident(names.fromString("super")),
                                                names.fromString("setOrderBy")
                                        ),
                                        List.of(
                                                treeMaker.Ident(names.fromString(schemaFieldName)),
                                                treeMaker.Ident(names.fromString(tableFieldName)),
                                                treeMaker.Ident(names.fromString(columnFieldName)),
                                                treeMaker.Ident(names.fromString("order"))
                                        )
                                )
                        ),
                        treeMaker.Return(
                                treeMaker.Ident(names.fromString("this"))
                        )
                )),
                null
        );
        builderClassDecl.defs = builderClassDecl.defs.append(orderByMethod);
    }

    private void addLimitMethod() {
        JCTree.JCMethodDecl limitMethod = treeMaker.MethodDef(
                treeMaker.Modifiers(Flags.PUBLIC | Flags.FINAL),
                names.fromString("limit"),
                treeMaker.Ident(names.fromString(getBuilderClassName())),
                List.nil(),
                List.of(
                        treeMaker.VarDef(
                                treeMaker.Modifiers(Flags.PARAMETER),
                                names.fromString("limit"),
                                treeMaker.TypeIdent(TypeTag.INT),
                                null
                        )
                ),
                List.nil(),
                treeMaker.Block(0, List.of(
                        treeMaker.Exec(
                                treeMaker.Apply(
                                        List.nil(),
                                        treeMaker.Select(
                                                treeMaker.Ident(names.fromString("super")),
                                                names.fromString("setLimit")
                                        ),
                                        List.of(
                                                treeMaker.Ident(names.fromString("limit"))
                                        )
                                )
                        ),
                        treeMaker.Return(
                                treeMaker.Ident(names.fromString("this"))
                        )
                )),
                null
        );
        builderClassDecl.defs = builderClassDecl.defs.append(limitMethod);
    }

    private void addOffsetMethod() {
        JCTree.JCMethodDecl offsetMethod = treeMaker.MethodDef(
                treeMaker.Modifiers(Flags.PUBLIC | Flags.FINAL),
                names.fromString("offset"),
                treeMaker.Ident(names.fromString(getBuilderClassName())),
                List.nil(),
                List.of(
                        treeMaker.VarDef(
                                treeMaker.Modifiers(Flags.PARAMETER),
                                names.fromString("offset"),
                                treeMaker.TypeIdent(TypeTag.INT),
                                null
                        )
                ),
                List.nil(),
                treeMaker.Block(0, List.of(
                        treeMaker.Exec(
                                treeMaker.Apply(
                                        List.nil(),
                                        treeMaker.Select(
                                                treeMaker.Ident(names.fromString("super")),
                                                names.fromString("setOffset")
                                        ),
                                        List.of(
                                                treeMaker.Ident(names.fromString("offset"))
                                        )
                                )
                        ),
                        treeMaker.Return(
                                treeMaker.Ident(names.fromString("this"))
                        )
                )),
                null
        );
        builderClassDecl.defs = builderClassDecl.defs.append(offsetMethod);
    }

    private void addWhereMethod() {
        JCTree.JCMethodDecl whereMethod = treeMaker.MethodDef(
                treeMaker.Modifiers(Flags.PUBLIC | Flags.FINAL),
                names.fromString("where"),
                treeMaker.Ident(names.fromString(getBuilderClassName())),
                List.nil(),
                List.of(
                        treeMaker.VarDef(
                                treeMaker.Modifiers(Flags.PARAMETER),
                                names.fromString("function"),
                                treeMaker.TypeApply(
                                        treeMaker.Ident(names.fromString("Function")),
                                        List.of(
                                                treeMaker.Ident(names.fromString(whereClassName)),
                                                treeMaker.Ident(names.fromString(whereClassName))
                                        )
                                ),
                                null
                        )
                ),
                List.nil(),
                treeMaker.Block(0, List.of(
                        treeMaker.Exec(
                                treeMaker.Apply(
                                        List.nil(),
                                        treeMaker.Select(
                                                treeMaker.Ident(names.fromString("function")),
                                                names.fromString("apply")
                                        ),
                                        List.of(
                                                treeMaker.Select(
                                                        treeMaker.Ident(names.fromString("super")),
                                                        names.fromString("where")
                                                )
                                        )
                                )
                        ),
                        treeMaker.Return(
                                treeMaker.Ident(names.fromString("this"))
                        )
                )),
                null
        );
        builderClassDecl.defs = builderClassDecl.defs.append(whereMethod);
    }

    class QueryWhereProcessor extends AbstractBuilderProcessor {
        private String dataSchemaFieldName;
        private String dataTableFieldName;

        public QueryWhereProcessor(JCTree.JCCompilationUnit compilationUnit, TreeMaker treeMaker, Names names, JCTree.JCClassDecl dataClassDecl, ParsedDataAnnotation dataAnnotation) {
            super(compilationUnit, treeMaker, names, dataClassDecl, dataAnnotation, "QueryWhere", null);
        }

        @Override
        protected void addImports() {
            JavaCPluginUtils.importClass(compilationUnit, treeMaker, names, "net.staticstudios.data.query", "BaseQueryWhere");
        }

        @Override
        protected @Nullable SuperClass extending() {
            return new SuperClass(
                    "BaseQueryWhere",
                    List.nil(),
                    List.nil()
            );
        }

        @Override
        protected void process() {
            dataSchemaFieldName = storeSchema("data", dataAnnotation.getSchema());
            dataTableFieldName = storeTable("data", dataAnnotation.getTable());

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

            addIsMethod(schemaFieldName, tableFieldName, columnFieldName, pv.getFieldName(), pv.getType(), fpv);
            addIsNotMethod(schemaFieldName, tableFieldName, columnFieldName, pv.getFieldName(), pv.getType(), fpv);

            addIsInCollectionMethod(schemaFieldName, tableFieldName, columnFieldName, pv.getFieldName(), pv.getType(), fpv);
            addIsInArrayMethod(schemaFieldName, tableFieldName, columnFieldName, pv.getFieldName(), pv.getType(), fpv);
            addIsNotInCollectionMethod(schemaFieldName, tableFieldName, columnFieldName, pv.getFieldName(), pv.getType(), fpv);
            addIsNotInArrayMethod(schemaFieldName, tableFieldName, columnFieldName, pv.getFieldName(), pv.getType(), fpv);

            if (pv.isNullable()) {
                addIsNullMethod(schemaFieldName, tableFieldName, columnFieldName, pv.getFieldName(), fpv);
                addIsNotNullMethod(schemaFieldName, tableFieldName, columnFieldName, pv.getFieldName(), fpv);
            }

            if (JavaCPluginUtils.isType(pv.getType(), String.class)) {
                addIsLikeMethod(schemaFieldName, tableFieldName, columnFieldName, pv.getFieldName(), fpv);
                addIsNotLikeMethod(schemaFieldName, tableFieldName, columnFieldName, pv.getFieldName(), fpv);
            }

            if (JavaCPluginUtils.isNumericType(pv.getType()) || JavaCPluginUtils.isType(pv.getType(), Timestamp.class)) {
                addIsLessThanMethod(schemaFieldName, tableFieldName, columnFieldName, pv.getFieldName(), pv.getType(), fpv);
                addIsLessThanOrEqualToMethod(schemaFieldName, tableFieldName, columnFieldName, pv.getFieldName(), pv.getType(), fpv);
                addIsGreaterThanMethod(schemaFieldName, tableFieldName, columnFieldName, pv.getFieldName(), pv.getType(), fpv);
                addIsGreaterThanOrEqualToMethod(schemaFieldName, tableFieldName, columnFieldName, pv.getFieldName(), pv.getType(), fpv);
                addIsBetweenMethod(schemaFieldName, tableFieldName, columnFieldName, pv.getFieldName(), pv.getType(), fpv);
                addIsNotBetweenMethod(schemaFieldName, tableFieldName, columnFieldName, pv.getFieldName(), pv.getType(), fpv);
            }
        }

        private List<JCTree.JCStatement> clause(@Nullable ParsedForeignPersistentValue fpv, JCTree.JCStatement... statements) {
            java.util.List<JCTree.JCStatement> list = new ArrayList<>();

            if (fpv != null) {
                String referencedSchemaFieldName = getStoredSchemaFieldName(fpv.getFieldName());
                String referencedTableFieldName = getStoredTableFieldName(fpv.getFieldName());
                String referencedColumnsFieldName = getStoredReferencedColumnsFieldName(fpv.getFieldName());
                String referringColumnsFieldName = getStoredReferringColumnsFieldName(fpv.getFieldName());
                list.add(
                        treeMaker.Exec(
                                treeMaker.Apply(
                                        List.nil(),
                                        treeMaker.Select(
                                                treeMaker.Ident(names.fromString("super")),
                                                names.fromString("addInnerJoin")
                                        ),
                                        List.of(
                                                treeMaker.Ident(names.fromString(dataSchemaFieldName)),
                                                treeMaker.Ident(names.fromString(dataTableFieldName)),
                                                treeMaker.Ident(names.fromString(referringColumnsFieldName)),
                                                treeMaker.Ident(names.fromString(referencedSchemaFieldName)),
                                                treeMaker.Ident(names.fromString(referencedTableFieldName)),
                                                treeMaker.Ident(names.fromString(referencedColumnsFieldName)
                                                )
                                        )
                                )
                        )
                );
            }

            list.addAll(Arrays.asList(statements));

            return List.from(list);
        }

        private void addIsMethod(String schemaFieldName, String tableFieldName, String columnFieldName, String fieldName, JCTree.JCExpression type, @Nullable ParsedForeignPersistentValue fpv) {
            JCTree.JCMethodDecl isMethod = treeMaker.MethodDef(
                    treeMaker.Modifiers(Flags.PUBLIC | Flags.FINAL),
                    names.fromString(fieldName + "Is"),
                    treeMaker.Ident(names.fromString(getBuilderClassName())),
                    List.nil(),
                    List.of(
                            treeMaker.VarDef(
                                    treeMaker.Modifiers(Flags.PARAMETER),
                                    names.fromString(fieldName),
                                    type,
                                    null
                            )
                    ),
                    List.nil(),
                    treeMaker.Block(0, clause(fpv,
                            treeMaker.Exec(
                                    treeMaker.Apply(
                                            List.nil(),
                                            treeMaker.Select(
                                                    treeMaker.Ident(names.fromString("super")),
                                                    names.fromString("equalsClause")
                                            ),
                                            List.of(
                                                    treeMaker.Ident(names.fromString(schemaFieldName)),
                                                    treeMaker.Ident(names.fromString(tableFieldName)),
                                                    treeMaker.Ident(names.fromString(columnFieldName)),
                                                    treeMaker.Ident(names.fromString(fieldName))
                                            )
                                    )
                            ),
                            treeMaker.Return(
                                    treeMaker.Ident(names.fromString("this"))
                            )
                    )),
                    null
            );
            builderClassDecl.defs = builderClassDecl.defs.append(isMethod);
        }

        private void addIsNotMethod(String schemaFieldName, String tableFieldName, String columnFieldName, String fieldName, JCTree.JCExpression type, @Nullable ParsedForeignPersistentValue fpv) {
            JCTree.JCMethodDecl isNotMethod = treeMaker.MethodDef(
                    treeMaker.Modifiers(Flags.PUBLIC | Flags.FINAL),
                    names.fromString(fieldName + "IsNot"),
                    treeMaker.Ident(names.fromString(getBuilderClassName())),
                    List.nil(),
                    List.of(
                            treeMaker.VarDef(
                                    treeMaker.Modifiers(Flags.PARAMETER),
                                    names.fromString(fieldName),
                                    type,
                                    null
                            )
                    ),
                    List.nil(),
                    treeMaker.Block(0, clause(fpv,
                            treeMaker.Exec(
                                    treeMaker.Apply(
                                            List.nil(),
                                            treeMaker.Select(
                                                    treeMaker.Ident(names.fromString("super")),
                                                    names.fromString("equalsClause")
                                            ),
                                            List.of(
                                                    treeMaker.Ident(names.fromString(schemaFieldName)),
                                                    treeMaker.Ident(names.fromString(tableFieldName)),
                                                    treeMaker.Ident(names.fromString(columnFieldName)),
                                                    treeMaker.Ident(names.fromString(fieldName))
                                            )
                                    )
                            ),
                            treeMaker.Return(
                                    treeMaker.Ident(names.fromString("this"))
                            )
                    )),
                    null
            );
            builderClassDecl.defs = builderClassDecl.defs.append(isNotMethod);
        }

        private void addIsNullMethod(String schemaFieldName, String tableFieldName, String columnFieldName, String fieldName, @Nullable ParsedForeignPersistentValue fpv) {
            JCTree.JCMethodDecl isNullMethod = treeMaker.MethodDef(
                    treeMaker.Modifiers(Flags.PUBLIC | Flags.FINAL),
                    names.fromString(fieldName + "IsNull"),
                    treeMaker.Ident(names.fromString(getBuilderClassName())),
                    List.nil(),
                    List.nil(),
                    List.nil(),
                    treeMaker.Block(0, clause(fpv,
                            treeMaker.Exec(
                                    treeMaker.Apply(
                                            List.nil(),
                                            treeMaker.Select(
                                                    treeMaker.Ident(names.fromString("super")),
                                                    names.fromString("nullClause")
                                            ),
                                            List.of(
                                                    treeMaker.Ident(names.fromString(schemaFieldName)),
                                                    treeMaker.Ident(names.fromString(tableFieldName)),
                                                    treeMaker.Ident(names.fromString(columnFieldName))
                                            )
                                    )
                            ),
                            treeMaker.Return(
                                    treeMaker.Ident(names.fromString("this"))
                            )
                    )),
                    null
            );
            builderClassDecl.defs = builderClassDecl.defs.append(isNullMethod);
        }

        private void addIsNotNullMethod(String schemaFieldName, String tableFieldName, String columnFieldName, String fieldName, @Nullable ParsedForeignPersistentValue fpv) {
            JCTree.JCMethodDecl isNotNullMethod = treeMaker.MethodDef(
                    treeMaker.Modifiers(Flags.PUBLIC | Flags.FINAL),
                    names.fromString(fieldName + "IsNotNull"),
                    treeMaker.Ident(names.fromString(getBuilderClassName())),
                    List.nil(),
                    List.nil(),
                    List.nil(),
                    treeMaker.Block(0, clause(fpv,
                            treeMaker.Exec(
                                    treeMaker.Apply(
                                            List.nil(),
                                            treeMaker.Select(
                                                    treeMaker.Ident(names.fromString("super")),
                                                    names.fromString("notNullClause")
                                            ),
                                            List.of(
                                                    treeMaker.Ident(names.fromString(schemaFieldName)),
                                                    treeMaker.Ident(names.fromString(tableFieldName)),
                                                    treeMaker.Ident(names.fromString(columnFieldName))
                                            )
                                    )
                            ),
                            treeMaker.Return(
                                    treeMaker.Ident(names.fromString("this"))
                            )
                    )),
                    null
            );
            builderClassDecl.defs = builderClassDecl.defs.append(isNotNullMethod);
        }

        private void addIsInCollectionMethod(String schemaFieldName, String tableFieldName, String columnFieldName, String fieldName, JCTree.JCExpression type, @Nullable ParsedForeignPersistentValue fpv) {
            JCTree.JCMethodDecl isInMethod = treeMaker.MethodDef(
                    treeMaker.Modifiers(Flags.PUBLIC | Flags.FINAL),
                    names.fromString(fieldName + "IsIn"),
                    treeMaker.Ident(names.fromString(getBuilderClassName())),
                    List.nil(),
                    List.of(
                            treeMaker.VarDef(
                                    treeMaker.Modifiers(Flags.PARAMETER),
                                    names.fromString(fieldName),
                                    treeMaker.TypeApply(
                                            treeMaker.Ident(names.fromString("Collection")),
                                            List.of(type)
                                    ),
                                    null
                            )
                    ),
                    List.nil(),
                    treeMaker.Block(0, clause(fpv,
                            treeMaker.Exec(
                                    treeMaker.Apply(
                                            List.nil(),
                                            treeMaker.Select(
                                                    treeMaker.Ident(names.fromString("super")),
                                                    names.fromString("inClause")
                                            ),
                                            List.of(
                                                    treeMaker.Ident(names.fromString(schemaFieldName)),
                                                    treeMaker.Ident(names.fromString(tableFieldName)),
                                                    treeMaker.Ident(names.fromString(columnFieldName)),
                                                    treeMaker.Apply(
                                                            List.nil(),
                                                            treeMaker.Select(
                                                                    treeMaker.Ident(names.fromString(fieldName)),
                                                                    names.fromString("toArray")
                                                            ),
                                                            List.nil()
                                                    )
                                            )
                                    )
                            ),
                            treeMaker.Return(
                                    treeMaker.Ident(names.fromString("this"))
                            )
                    )),
                    null
            );
            builderClassDecl.defs = builderClassDecl.defs.append(isInMethod);
        }

        private void addIsInArrayMethod(String schemaFieldName, String tableFieldName, String columnFieldName, String fieldName, JCTree.JCExpression type, @Nullable ParsedForeignPersistentValue fpv) {
            JCTree.JCMethodDecl isInMethod = treeMaker.MethodDef(
                    treeMaker.Modifiers(Flags.PUBLIC | Flags.FINAL),
                    names.fromString(fieldName + "IsIn"),
                    treeMaker.Ident(names.fromString(getBuilderClassName())),
                    List.nil(),
                    List.of(
                            treeMaker.VarDef(
                                    treeMaker.Modifiers(Flags.PARAMETER | Flags.VARARGS),
                                    names.fromString(fieldName),
                                    treeMaker.TypeArray(type),
                                    null
                            )
                    ),
                    List.nil(),
                    treeMaker.Block(0, clause(fpv,
                            treeMaker.Exec(
                                    treeMaker.Apply(
                                            List.nil(),
                                            treeMaker.Select(
                                                    treeMaker.Ident(names.fromString("super")),
                                                    names.fromString("inClause")
                                            ),
                                            List.of(
                                                    treeMaker.Ident(names.fromString(schemaFieldName)),
                                                    treeMaker.Ident(names.fromString(tableFieldName)),
                                                    treeMaker.Ident(names.fromString(columnFieldName)),
                                                    treeMaker.Ident(names.fromString(fieldName))
                                            )
                                    )
                            ),
                            treeMaker.Return(
                                    treeMaker.Ident(names.fromString("this"))
                            )
                    )),
                    null
            );
            builderClassDecl.defs = builderClassDecl.defs.append(isInMethod);
        }

        private void addIsNotInCollectionMethod(String schemaFieldName, String tableFieldName, String columnFieldName, String fieldName, JCTree.JCExpression type, @Nullable ParsedForeignPersistentValue fpv) {
            JCTree.JCMethodDecl isNotInMethod = treeMaker.MethodDef(
                    treeMaker.Modifiers(Flags.PUBLIC | Flags.FINAL),
                    names.fromString(fieldName + "IsNotIn"),
                    treeMaker.Ident(names.fromString(getBuilderClassName())),
                    List.nil(),
                    List.of(
                            treeMaker.VarDef(
                                    treeMaker.Modifiers(Flags.PARAMETER),
                                    names.fromString(fieldName),
                                    treeMaker.TypeApply(
                                            treeMaker.Ident(names.fromString("Collection")),
                                            List.of(type)
                                    ),
                                    null
                            )
                    ),
                    List.nil(),
                    treeMaker.Block(0, clause(fpv,
                            treeMaker.Exec(
                                    treeMaker.Apply(
                                            List.nil(),
                                            treeMaker.Select(
                                                    treeMaker.Ident(names.fromString("super")),
                                                    names.fromString("notInClause")
                                            ),
                                            List.of(
                                                    treeMaker.Ident(names.fromString(schemaFieldName)),
                                                    treeMaker.Ident(names.fromString(tableFieldName)),
                                                    treeMaker.Ident(names.fromString(columnFieldName)),
                                                    treeMaker.Apply(
                                                            List.nil(),
                                                            treeMaker.Select(
                                                                    treeMaker.Ident(names.fromString(fieldName)),
                                                                    names.fromString("toArray")
                                                            ),
                                                            List.nil()
                                                    )
                                            )
                                    )
                            ),
                            treeMaker.Return(
                                    treeMaker.Ident(names.fromString("this"))
                            )
                    )),
                    null
            );
            builderClassDecl.defs = builderClassDecl.defs.append(isNotInMethod);
        }

        private void addIsNotInArrayMethod(String schemaFieldName, String tableFieldName, String columnFieldName, String fieldName, JCTree.JCExpression type, @Nullable ParsedForeignPersistentValue fpv) {
            JCTree.JCMethodDecl isNotInMethod = treeMaker.MethodDef(
                    treeMaker.Modifiers(Flags.PUBLIC | Flags.FINAL),
                    names.fromString(fieldName + "IsNotIn"),
                    treeMaker.Ident(names.fromString(getBuilderClassName())),
                    List.nil(),
                    List.of(
                            treeMaker.VarDef(
                                    treeMaker.Modifiers(Flags.PARAMETER | Flags.VARARGS),
                                    names.fromString(fieldName),
                                    treeMaker.TypeArray(type),
                                    null
                            )
                    ),
                    List.nil(),
                    treeMaker.Block(0, clause(fpv,
                            treeMaker.Exec(
                                    treeMaker.Apply(
                                            List.nil(),
                                            treeMaker.Select(
                                                    treeMaker.Ident(names.fromString("super")),
                                                    names.fromString("notInClause")
                                            ),
                                            List.of(
                                                    treeMaker.Ident(names.fromString(schemaFieldName)),
                                                    treeMaker.Ident(names.fromString(tableFieldName)),
                                                    treeMaker.Ident(names.fromString(columnFieldName)),
                                                    treeMaker.Ident(names.fromString(fieldName))
                                            )
                                    )
                            ),
                            treeMaker.Return(
                                    treeMaker.Ident(names.fromString("this"))
                            )
                    )),
                    null
            );
            builderClassDecl.defs = builderClassDecl.defs.append(isNotInMethod);
        }

        private void addIsLikeMethod(String schemaFieldName, String tableFieldName, String columnFieldName, String fieldName, @Nullable ParsedForeignPersistentValue fpv) {
            JCTree.JCMethodDecl isLikeMethod = treeMaker.MethodDef(
                    treeMaker.Modifiers(Flags.PUBLIC | Flags.FINAL),
                    names.fromString(fieldName + "IsLike"),
                    treeMaker.Ident(names.fromString(getBuilderClassName())),
                    List.nil(),
                    List.of(
                            treeMaker.VarDef(
                                    treeMaker.Modifiers(Flags.PARAMETER),
                                    names.fromString("pattern"),
                                    treeMaker.Ident(names.fromString("String")),
                                    null
                            )
                    ),
                    List.nil(),
                    treeMaker.Block(0, clause(fpv,
                            treeMaker.Exec(
                                    treeMaker.Apply(
                                            List.nil(),
                                            treeMaker.Select(
                                                    treeMaker.Ident(names.fromString("super")),
                                                    names.fromString("likeClause")
                                            ),
                                            List.of(
                                                    treeMaker.Ident(names.fromString(schemaFieldName)),
                                                    treeMaker.Ident(names.fromString(tableFieldName)),
                                                    treeMaker.Ident(names.fromString(columnFieldName)),
                                                    treeMaker.Ident(names.fromString("pattern"))
                                            )
                                    )
                            ),
                            treeMaker.Return(
                                    treeMaker.Ident(names.fromString("this"))
                            )
                    )),
                    null
            );
            builderClassDecl.defs = builderClassDecl.defs.append(isLikeMethod);
        }

        private void addIsNotLikeMethod(String schemaFieldName, String tableFieldName, String columnFieldName, String fieldName, @Nullable ParsedForeignPersistentValue fpv) {
            JCTree.JCMethodDecl isNotLikeMethod = treeMaker.MethodDef(
                    treeMaker.Modifiers(Flags.PUBLIC | Flags.FINAL),
                    names.fromString(fieldName + "IsNotLike"),
                    treeMaker.Ident(names.fromString(getBuilderClassName())),
                    List.nil(),
                    List.of(
                            treeMaker.VarDef(
                                    treeMaker.Modifiers(Flags.PARAMETER),
                                    names.fromString("pattern"),
                                    treeMaker.Ident(names.fromString("String")),
                                    null
                            )
                    ),
                    List.nil(),
                    treeMaker.Block(0, clause(fpv,
                            treeMaker.Exec(
                                    treeMaker.Apply(
                                            List.nil(),
                                            treeMaker.Select(
                                                    treeMaker.Ident(names.fromString("super")),
                                                    names.fromString("notLikeClause")
                                            ),
                                            List.of(
                                                    treeMaker.Ident(names.fromString(schemaFieldName)),
                                                    treeMaker.Ident(names.fromString(tableFieldName)),
                                                    treeMaker.Ident(names.fromString(columnFieldName)),
                                                    treeMaker.Ident(names.fromString("pattern"))
                                            )
                                    )
                            ),
                            treeMaker.Return(
                                    treeMaker.Ident(names.fromString("this"))
                            )
                    )),
                    null
            );
            builderClassDecl.defs = builderClassDecl.defs.append(isNotLikeMethod);
        }

        private void addIsLessThanMethod(String schemaFieldName, String tableFieldName, String columnFieldName, String fieldName, JCTree.JCExpression type, @Nullable ParsedForeignPersistentValue fpv) {
            JCTree.JCMethodDecl lessThanMethod = treeMaker.MethodDef(
                    treeMaker.Modifiers(Flags.PUBLIC | Flags.FINAL),
                    names.fromString(fieldName + "IsLessThan"),
                    treeMaker.Ident(names.fromString(getBuilderClassName())),
                    List.nil(),
                    List.of(
                            treeMaker.VarDef(
                                    treeMaker.Modifiers(Flags.PARAMETER),
                                    names.fromString(fieldName),
                                    type,
                                    null
                            )
                    ),
                    List.nil(),
                    treeMaker.Block(0, clause(fpv,
                            treeMaker.Exec(
                                    treeMaker.Apply(
                                            List.nil(),
                                            treeMaker.Select(
                                                    treeMaker.Ident(names.fromString("super")),
                                                    names.fromString("lessThanClause")
                                            ),
                                            List.of(
                                                    treeMaker.Ident(names.fromString(schemaFieldName)),
                                                    treeMaker.Ident(names.fromString(tableFieldName)),
                                                    treeMaker.Ident(names.fromString(columnFieldName)),
                                                    treeMaker.Ident(names.fromString(fieldName))
                                            )
                                    )
                            ),
                            treeMaker.Return(
                                    treeMaker.Ident(names.fromString("this"))
                            )
                    )),
                    null
            );
            builderClassDecl.defs = builderClassDecl.defs.append(lessThanMethod);
        }

        private void addIsLessThanOrEqualToMethod(String schemaFieldName, String tableFieldName, String columnFieldName, String fieldName, JCTree.JCExpression type, @Nullable ParsedForeignPersistentValue fpv) {
            JCTree.JCMethodDecl lessThanOrEqualToMethod = treeMaker.MethodDef(
                    treeMaker.Modifiers(Flags.PUBLIC | Flags.FINAL),
                    names.fromString(fieldName + "IsLessThanOrEqualTo"),
                    treeMaker.Ident(names.fromString(getBuilderClassName())),
                    List.nil(),
                    List.of(
                            treeMaker.VarDef(
                                    treeMaker.Modifiers(Flags.PARAMETER),
                                    names.fromString(fieldName),
                                    type,
                                    null
                            )
                    ),
                    List.nil(),
                    treeMaker.Block(0, clause(fpv,
                            treeMaker.Exec(
                                    treeMaker.Apply(
                                            List.nil(),
                                            treeMaker.Select(
                                                    treeMaker.Ident(names.fromString("super")),
                                                    names.fromString("lessThanOrEqualToClause")
                                            ),
                                            List.of(
                                                    treeMaker.Ident(names.fromString(schemaFieldName)),
                                                    treeMaker.Ident(names.fromString(tableFieldName)),
                                                    treeMaker.Ident(names.fromString(columnFieldName)),
                                                    treeMaker.Ident(names.fromString(fieldName))
                                            )
                                    )
                            ),
                            treeMaker.Return(
                                    treeMaker.Ident(names.fromString("this"))
                            )
                    )),
                    null
            );
            builderClassDecl.defs = builderClassDecl.defs.append(lessThanOrEqualToMethod);
        }

        private void addIsGreaterThanMethod(String schemaFieldName, String tableFieldName, String columnFieldName, String fieldName, JCTree.JCExpression type, @Nullable ParsedForeignPersistentValue fpv) {
            JCTree.JCMethodDecl greaterThanMethod = treeMaker.MethodDef(
                    treeMaker.Modifiers(Flags.PUBLIC | Flags.FINAL),
                    names.fromString(fieldName + "IsGreaterThan"),
                    treeMaker.Ident(names.fromString(getBuilderClassName())),
                    List.nil(),
                    List.of(
                            treeMaker.VarDef(
                                    treeMaker.Modifiers(Flags.PARAMETER),
                                    names.fromString(fieldName),
                                    type,
                                    null
                            )
                    ),
                    List.nil(),
                    treeMaker.Block(0, clause(fpv,
                            treeMaker.Exec(
                                    treeMaker.Apply(
                                            List.nil(),
                                            treeMaker.Select(
                                                    treeMaker.Ident(names.fromString("super")),
                                                    names.fromString("greaterThanClause")
                                            ),
                                            List.of(
                                                    treeMaker.Ident(names.fromString(schemaFieldName)),
                                                    treeMaker.Ident(names.fromString(tableFieldName)),
                                                    treeMaker.Ident(names.fromString(columnFieldName)),
                                                    treeMaker.Ident(names.fromString(fieldName))
                                            )
                                    )
                            ),
                            treeMaker.Return(
                                    treeMaker.Ident(names.fromString("this"))
                            )
                    )),
                    null
            );
            builderClassDecl.defs = builderClassDecl.defs.append(greaterThanMethod);
        }

        private void addIsGreaterThanOrEqualToMethod(String schemaFieldName, String tableFieldName, String columnFieldName, String fieldName, JCTree.JCExpression type, @Nullable ParsedForeignPersistentValue fpv) {
            JCTree.JCMethodDecl greaterThanOrEqualToMethod = treeMaker.MethodDef(
                    treeMaker.Modifiers(Flags.PUBLIC | Flags.FINAL),
                    names.fromString(fieldName + "IsGreaterThanOrEqualTo"),
                    treeMaker.Ident(names.fromString(getBuilderClassName())),
                    List.nil(),
                    List.of(
                            treeMaker.VarDef(
                                    treeMaker.Modifiers(Flags.PARAMETER),
                                    names.fromString(fieldName),
                                    type,
                                    null
                            )
                    ),
                    List.nil(),
                    treeMaker.Block(0, clause(fpv,
                            treeMaker.Exec(
                                    treeMaker.Apply(
                                            List.nil(),
                                            treeMaker.Select(
                                                    treeMaker.Ident(names.fromString("super")),
                                                    names.fromString("greaterThanOrEqualToClause")
                                            ),
                                            List.of(
                                                    treeMaker.Ident(names.fromString(schemaFieldName)),
                                                    treeMaker.Ident(names.fromString(tableFieldName)),
                                                    treeMaker.Ident(names.fromString(columnFieldName)),
                                                    treeMaker.Ident(names.fromString(fieldName))
                                            )
                                    )
                            ),
                            treeMaker.Return(
                                    treeMaker.Ident(names.fromString("this"))
                            )
                    )),
                    null
            );
            builderClassDecl.defs = builderClassDecl.defs.append(greaterThanOrEqualToMethod);
        }

        private void addIsBetweenMethod(String schemaFieldName, String tableFieldName, String columnFieldName, String fieldName, JCTree.JCExpression type, @Nullable ParsedForeignPersistentValue fpv) {
            JCTree.JCMethodDecl isBetweenMethod = treeMaker.MethodDef(
                    treeMaker.Modifiers(Flags.PUBLIC | Flags.FINAL),
                    names.fromString(fieldName + "IsBetween"),
                    treeMaker.Ident(names.fromString(getBuilderClassName())),
                    List.nil(),
                    List.of(
                            treeMaker.VarDef(
                                    treeMaker.Modifiers(Flags.PARAMETER),
                                    names.fromString("min"),
                                    type,
                                    null
                            ),
                            treeMaker.VarDef(
                                    treeMaker.Modifiers(Flags.PARAMETER),
                                    names.fromString("max"),
                                    type,
                                    null
                            )
                    ),
                    List.nil(),
                    treeMaker.Block(0, clause(fpv,
                            treeMaker.Exec(
                                    treeMaker.Apply(
                                            List.nil(),
                                            treeMaker.Select(
                                                    treeMaker.Ident(names.fromString("super")),
                                                    names.fromString("betweenClause")
                                            ),
                                            List.of(
                                                    treeMaker.Ident(names.fromString(schemaFieldName)),
                                                    treeMaker.Ident(names.fromString(tableFieldName)),
                                                    treeMaker.Ident(names.fromString(columnFieldName)),
                                                    treeMaker.Ident(names.fromString("min")),
                                                    treeMaker.Ident(names.fromString("max"))
                                            )
                                    )
                            ),
                            treeMaker.Return(
                                    treeMaker.Ident(names.fromString("this"))
                            )
                    )),
                    null
            );
            builderClassDecl.defs = builderClassDecl.defs.append(isBetweenMethod);
        }

        private void addIsNotBetweenMethod(String schemaFieldName, String tableFieldName, String columnFieldName, String fieldName, JCTree.JCExpression type, @Nullable ParsedForeignPersistentValue fpv) {
            JCTree.JCMethodDecl isNotBetweenMethod = treeMaker.MethodDef(
                    treeMaker.Modifiers(Flags.PUBLIC | Flags.FINAL),
                    names.fromString(fieldName + "IsNotBetween"),
                    treeMaker.Ident(names.fromString(getBuilderClassName())),
                    List.nil(),
                    List.of(
                            treeMaker.VarDef(
                                    treeMaker.Modifiers(Flags.PARAMETER),
                                    names.fromString("min"),
                                    type,
                                    null
                            ),
                            treeMaker.VarDef(
                                    treeMaker.Modifiers(Flags.PARAMETER),
                                    names.fromString("max"),
                                    type,
                                    null
                            )
                    ),
                    List.nil(),
                    treeMaker.Block(0, clause(fpv,
                            treeMaker.Exec(
                                    treeMaker.Apply(
                                            List.nil(),
                                            treeMaker.Select(
                                                    treeMaker.Ident(names.fromString("super")),
                                                    names.fromString("notBetweenClause")
                                            ),
                                            List.of(
                                                    treeMaker.Ident(names.fromString(schemaFieldName)),
                                                    treeMaker.Ident(names.fromString(tableFieldName)),
                                                    treeMaker.Ident(names.fromString(columnFieldName)),
                                                    treeMaker.Ident(names.fromString("min")),
                                                    treeMaker.Ident(names.fromString("max"))
                                            )
                                    )
                            ),
                            treeMaker.Return(
                                    treeMaker.Ident(names.fromString("this"))
                            )
                    )),
                    null
            );
            builderClassDecl.defs = builderClassDecl.defs.append(isNotBetweenMethod);
        }

        private void addGroupMethod() {
            JCTree.JCMethodDecl groupMethod = treeMaker.MethodDef(
                    treeMaker.Modifiers(Flags.PUBLIC | Flags.FINAL),
                    names.fromString("group"),
                    treeMaker.Ident(names.fromString(getBuilderClassName())),
                    List.nil(),
                    List.of(
                            treeMaker.VarDef(
                                    treeMaker.Modifiers(Flags.PARAMETER),
                                    names.fromString("function"),
                                    treeMaker.TypeApply(
                                            treeMaker.Ident(names.fromString("Function")),
                                            List.of(
                                                    treeMaker.Ident(names.fromString(getBuilderClassName())),
                                                    treeMaker.Ident(names.fromString(getBuilderClassName()))
                                            )
                                    ),
                                    null
                            )
                    ),
                    List.nil(),
                    treeMaker.Block(0, List.of(
                            treeMaker.Exec(
                                    treeMaker.Apply(
                                            List.nil(),
                                            treeMaker.Select(
                                                    treeMaker.Ident(names.fromString("super")),
                                                    names.fromString("pushGroup")
                                            ),
                                            List.nil()
                                    )
                            ),
                            treeMaker.Exec(
                                    treeMaker.Apply(
                                            List.nil(),
                                            treeMaker.Select(
                                                    treeMaker.Ident(names.fromString("function")),
                                                    names.fromString("apply")
                                            ),
                                            List.of(
                                                    treeMaker.Ident(names.fromString("this"))
                                            )
                                    )
                            ),
                            treeMaker.Exec(
                                    treeMaker.Apply(
                                            List.nil(),
                                            treeMaker.Select(
                                                    treeMaker.Ident(names.fromString("super")),
                                                    names.fromString("popGroup")
                                            ),
                                            List.nil()
                                    )
                            ),
                            treeMaker.Return(
                                    treeMaker.Ident(names.fromString("this"))
                            )
                    )),
                    null
            );
            builderClassDecl.defs = builderClassDecl.defs.append(groupMethod);
        }

        private void addAndMethod() {
            JCTree.JCMethodDecl andMethod = treeMaker.MethodDef(
                    treeMaker.Modifiers(Flags.PUBLIC | Flags.FINAL),
                    names.fromString("and"),
                    treeMaker.Ident(names.fromString(getBuilderClassName())),
                    List.nil(),
                    List.nil(),
                    List.nil(),
                    treeMaker.Block(0, List.of(
                            treeMaker.Exec(
                                    treeMaker.Apply(
                                            List.nil(),
                                            treeMaker.Select(
                                                    treeMaker.Ident(names.fromString("super")),
                                                    names.fromString("andClause")
                                            ),
                                            List.nil()
                                    )
                            ),
                            treeMaker.Return(
                                    treeMaker.Ident(names.fromString("this"))
                            )
                    )),
                    null
            );
            builderClassDecl.defs = builderClassDecl.defs.append(andMethod);
        }

        private void addOrMethod() {
            JCTree.JCMethodDecl andMethod = treeMaker.MethodDef(
                    treeMaker.Modifiers(Flags.PUBLIC | Flags.FINAL),
                    names.fromString("or"),
                    treeMaker.Ident(names.fromString(getBuilderClassName())),
                    List.nil(),
                    List.nil(),
                    List.nil(),
                    treeMaker.Block(0, List.of(
                            treeMaker.Exec(
                                    treeMaker.Apply(
                                            List.nil(),
                                            treeMaker.Select(
                                                    treeMaker.Ident(names.fromString("super")),
                                                    names.fromString("orClause")
                                            ),
                                            List.nil()
                                    )
                            ),
                            treeMaker.Return(
                                    treeMaker.Ident(names.fromString("this"))
                            )
                    )),
                    null
            );
            builderClassDecl.defs = builderClassDecl.defs.append(andMethod);
        }
    }
}
