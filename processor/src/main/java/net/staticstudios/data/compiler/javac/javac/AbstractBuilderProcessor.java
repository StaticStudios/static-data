package net.staticstudios.data.compiler.javac.javac;

import com.sun.source.util.Trees;
import com.sun.tools.javac.code.Flags;
import com.sun.tools.javac.code.Symbol;
import com.sun.tools.javac.comp.*;
import com.sun.tools.javac.tree.JCTree;
import com.sun.tools.javac.util.Context;
import com.sun.tools.javac.util.List;
import com.sun.tools.javac.util.Names;
import net.staticstudios.data.Data;
import net.staticstudios.data.compiler.javac.ProcessorContext;
import net.staticstudios.data.compiler.javac.util.TypeUtils;
import net.staticstudios.data.utils.Link;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Method;
import java.util.ArrayList;

public abstract class AbstractBuilderProcessor extends PositionedTreeMaker {
    protected final Context context;
    protected final Trees trees;
    protected final Names names;
    protected final Enter enter;
    protected final MemberEnter memberEnter;
    protected final TypeEnter typeEnter;
    protected final JCTree.JCClassDecl dataClassDecl;
    protected final Data dataAnnotation;
    protected final TypeUtils typeUtils;
    private final String builderClassSuffix;
    private final @Nullable String builderMethodName;
    protected JCTree.JCClassDecl builderClassDecl;

    public AbstractBuilderProcessor(
            ProcessorContext processorContext,
            String builderClassSuffix,
            @Nullable String builderMethodName
    ) {
        super(processorContext.context(), processorContext.dataClassDecl().pos);
        this.context = processorContext.context();
        this.trees = processorContext.trees();
        this.names = Names.instance(context);
        this.enter = Enter.instance(context);
        this.typeEnter = TypeEnter.instance(context);
        this.memberEnter = MemberEnter.instance(context);
        this.dataClassDecl = processorContext.dataClassDecl();
        this.dataAnnotation = processorContext.dataAnnotation();
        this.builderClassSuffix = builderClassSuffix;
        this.builderMethodName = builderMethodName;
        this.typeUtils = processorContext.typeUtils();
    }

    protected abstract void process();

    public void runProcessor() {
        if (dataClassDecl.defs.stream()
                .anyMatch(def -> def instanceof JCTree.JCClassDecl &&
                        ((JCTree.JCClassDecl) def).name.toString().equals(getBuilderClassName()))) {
            return;
        }

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
            classExtends = TypeApply(
                    chainDots(superClass.fqn().split("\\.")),
                    superClass.superParms()
            );
            superCall = Apply(
                    List.nil(),
                    Ident(names.fromString("super")),
                    superClass.args()
            );
        } else {
            classExtends = null;
            superCall = null;
        }

        builderClassDecl = createClass(ClassDef(
                Modifiers(Flags.PUBLIC | Flags.STATIC),
                names.fromString(getBuilderClassName()),
                List.nil(),
                classExtends,
                List.nil(),
                List.nil()
        ), dataClassDecl);


        java.util.List<JCTree.JCExpressionStatement> constructorBodyStatements = new ArrayList<>();
        if (superCall != null) {
            constructorBodyStatements.add(Exec(superCall));
        }

        if (this.builderMethodName != null) {
            createField(VarDef(
                    Modifiers(Flags.PRIVATE | Flags.FINAL),
                    names.fromString("dataManager"),
                    chainDots("net", "staticstudios", "data", "DataManager"),
                    null
            ), builderClassDecl);
            constructorBodyStatements.add(
                    Exec(
                            Assign(
                                    Select(
                                            Ident(names.fromString("this")),
                                            names.fromString("dataManager")
                                    ),
                                    Ident(names.fromString("dataManager"))
                            )
                    )
            );
        }

        createMethod(MethodDef(
                Modifiers(Flags.PUBLIC),
                names.fromString("<init>"),
                null,
                List.nil(),
                this.builderMethodName == null ?
                        List.nil()
                        :
                        List.of(
                                VarDef(
                                        Modifiers(Flags.PARAMETER),
                                        names.fromString("dataManager"),
                                        chainDots("net", "staticstudios", "data", "DataManager"),
                                        null
                                )
                        ),
                List.nil(),
                Block(0, List.from(constructorBodyStatements)),
                null
        ), builderClassDecl);
    }

    private void makeBuilderMethod() {
        createMethod(MethodDef(
                Modifiers(Flags.PUBLIC | Flags.STATIC),
                names.fromString(builderMethodName),
                Ident(names.fromString(getBuilderClassName())),
                List.nil(),
                List.nil(),
                List.nil(),
                Block(0, List.of(
                        Return(
                                Apply(
                                        List.nil(),
                                        Ident(names.fromString(builderMethodName)),
                                        List.of(
                                                Apply(
                                                        List.nil(),
                                                        Select(
                                                                chainDots("net", "staticstudios", "data", "DataManager"),
                                                                names.fromString("getInstance")
                                                        ),
                                                        List.nil()
                                                )
                                        )
                                )
                        )
                )),
                null
        ), dataClassDecl);
    }

    private void makeParameterizedBuilderMethod() {
        createMethod(MethodDef(
                Modifiers(Flags.PUBLIC | Flags.STATIC),
                names.fromString(builderMethodName),
                Ident(names.fromString(getBuilderClassName())),
                List.nil(),
                List.of(
                        VarDef(
                                Modifiers(Flags.PARAMETER),
                                names.fromString("dataManager"),
                                chainDots("net", "staticstudios", "data", "DataManager"),
                                null
                        )
                ),
                List.nil(),
                Block(0, List.of(
                        Return(
                                NewClass(null, List.nil(),
                                        Ident(names.fromString(getBuilderClassName())),
                                        List.of(
                                                Ident(names.fromString("dataManager"))
                                        ),
                                        null
                                )
                        )
                )),
                null
        ), dataClassDecl);
    }

    public String getBuilderClassName() {
        return dataClassDecl.name.toString() + builderClassSuffix;
    }

//    public JCTree.JCExpression getBuilderIdent() {
//        return chainDots(""
//    }

    public String storeSchema(String fieldName, String encoded) {
        String schemaFieldName = getStoredSchemaFieldName(fieldName);
        storeField(schemaFieldName, encoded);
        return schemaFieldName;
    }

    public String storeTable(String fieldName, String encoded) {
        String tableFieldName = getStoredTableFieldName(fieldName);
        storeField(tableFieldName, encoded);
        return tableFieldName;
    }

    public String storeColumn(String fieldName, String encoded) {
        String columnFieldName = getStoredColumnFieldName(fieldName);
        storeField(columnFieldName, encoded);
        return columnFieldName;
    }

    public void storeField(String fieldName, String encoded) {
        createField(VarDef(
                Modifiers(Flags.PRIVATE | Flags.STATIC | Flags.FINAL),
                names.fromString(fieldName),
                Ident(names.fromString("String")),
                Apply(
                        List.nil(),
                        Select(
                                chainDots("net", "staticstudios", "data", "util", "ValueUtils"),
                                names.fromString("parseValue")
                        ),
                        List.of(
                                Literal(encoded)
                        )
                )
        ), builderClassDecl);
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

        createField(VarDef(
                Modifiers(Flags.PRIVATE | Flags.FINAL),
                names.fromString(referringColumnsFieldName),
                TypeArray(Ident(names.fromString("String"))),
                NewArray(
                        Ident(names.fromString("String")),
                        List.nil(),
                        List.from(
                                links.stream().map(link ->
                                        Literal(link.columnInReferringTable())
                                ).toList()
                        )
                )
        ), builderClassDecl);

        createField(VarDef(
                Modifiers(Flags.PRIVATE | Flags.FINAL),
                names.fromString(referencedColumnsFieldName),
                TypeArray(Ident(names.fromString("String"))),
                NewArray(
                        Ident(names.fromString("String")),
                        List.nil(),
                        List.from(
                                links.stream().map(link ->
                                        Literal(link.columnInReferencedTable())
                                ).toList()
                        )
                )
        ), builderClassDecl);
    }

    public String getStoredReferringColumnsFieldName(String fieldName) {
        return fieldName + "$referringColumns";
    }

    public String getStoredReferencedColumnsFieldName(String fieldName) {
        return fieldName + "$referencedColumns";
    }

    public JCTree.JCClassDecl createClass(JCTree.JCClassDecl classDecl, JCTree.JCClassDecl containingClassDecl) {
        containingClassDecl.defs = containingClassDecl.defs.append(classDecl);
        Symbol.ClassSymbol owner = containingClassDecl.sym;
        Env<AttrContext> env = enter.getEnv(owner);

        if (env == null) {
            env = enter.getClassEnv(owner);
        }

//        classEnter(classDecl, env);
        return classDecl;
    }

    public JCTree.JCMethodDecl createMethod(JCTree.JCMethodDecl methodDecl, JCTree.JCClassDecl classDecl) {
        classDecl.defs = classDecl.defs.append(methodDecl);
        Symbol.ClassSymbol owner = classDecl.sym;
        Env<AttrContext> env = enter.getEnv(owner);

        if (env == null) {
            env = enter.getClassEnv(owner);
        }

//        memberEnter(methodDecl, env);

        return methodDecl;
    }

    public JCTree.JCVariableDecl createField(JCTree.JCVariableDecl fieldDecl, JCTree.JCClassDecl classDecl) {
        classDecl.defs = classDecl.defs.append(fieldDecl);

        Symbol.ClassSymbol owner = classDecl.sym;
        Env<AttrContext> env = enter.getEnv(owner);

        if (env == null) {
            env = enter.getClassEnv(owner);
        }
//        memberEnter(fieldDecl, env);

        return fieldDecl;
    }

    private void classEnter(JCTree tree, Env<AttrContext> env) {
        try {
            Method method = Enter.class.getDeclaredMethod("classEnter", JCTree.class, Env.class);
            method.setAccessible(true);
            method.invoke(enter, tree, env);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    private void classEnter(List<JCTree> trees, Env<AttrContext> env) {
        try {
            Method method = Enter.class.getDeclaredMethod("classEnter", List.class, Env.class);
            method.setAccessible(true);
            method.invoke(enter, trees, env);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void memberEnter(JCTree tree, Env<AttrContext> env) {
        try {
            Method method = MemberEnter.class.getDeclaredMethod("memberEnter", JCTree.class, Env.class);
            method.setAccessible(true);
            method.invoke(memberEnter, tree, env);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected void memberEnter(List<JCTree> trees, Env<AttrContext> env) {
        try {
            Method method = MemberEnter.class.getDeclaredMethod("memberEnter", List.class, Env.class);
            method.setAccessible(true);
            method.invoke(memberEnter, trees, env);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
