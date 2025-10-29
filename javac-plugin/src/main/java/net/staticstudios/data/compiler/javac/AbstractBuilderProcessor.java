package net.staticstudios.data.compiler.javac;

import com.sun.tools.javac.code.Flags;
import com.sun.tools.javac.tree.JCTree;
import com.sun.tools.javac.tree.TreeMaker;
import com.sun.tools.javac.util.List;
import com.sun.tools.javac.util.Names;

public abstract class AbstractBuilderProcessor {
    protected final JCTree.JCCompilationUnit compilationUnit;
    protected final TreeMaker treeMaker;
    protected final Names names;
    protected final JCTree.JCClassDecl dataClassDecl;
    protected final ParsedDataAnnotation dataAnnotation;
    private final String builderClassSuffix;
    private final String builderMethodName;
    protected JCTree.JCClassDecl builderClassDecl;

    public AbstractBuilderProcessor(JCTree.JCCompilationUnit compilationUnit, TreeMaker treeMaker, Names names, JCTree.JCClassDecl dataClassDecl, ParsedDataAnnotation dataAnnotation, String builderClassSuffix, String builderMethodName) {
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
        makeBuilderMethod();
        makeParameterizedBuilderMethod();
        process();
    }

    private void makeBuilderClass() {
        builderClassDecl = treeMaker.ClassDef(
                treeMaker.Modifiers(Flags.PUBLIC | Flags.STATIC),
                names.fromString(getBuilderClassName()),
                List.nil(),
                null,
                List.nil(),
                List.nil()
        );

        JCTree.JCVariableDecl dataManagerField = treeMaker.VarDef(
                treeMaker.Modifiers(Flags.PRIVATE | Flags.FINAL),
                names.fromString("dataManager"),
                treeMaker.Ident(names.fromString("DataManager")),
                null
        );
        builderClassDecl.defs = builderClassDecl.defs.append(dataManagerField);

        JCTree.JCMethodDecl constructor = treeMaker.MethodDef(
                treeMaker.Modifiers(Flags.PUBLIC),
                names.fromString("<init>"),
                null,
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
                        treeMaker.Exec(
                                treeMaker.Assign(
                                        treeMaker.Select(
                                                treeMaker.Ident(names.fromString("this")),
                                                names.fromString("dataManager")
                                        ),
                                        treeMaker.Ident(names.fromString("dataManager"))
                                )
                        )
                )),
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
}
