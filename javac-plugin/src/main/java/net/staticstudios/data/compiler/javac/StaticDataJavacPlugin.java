package net.staticstudios.data.compiler.javac;

import com.sun.source.tree.ClassTree;
import com.sun.source.tree.IdentifierTree;
import com.sun.source.tree.Tree;
import com.sun.source.util.*;
import com.sun.tools.javac.api.BasicJavacTask;
import com.sun.tools.javac.code.Flags;
import com.sun.tools.javac.tree.JCTree;
import com.sun.tools.javac.tree.TreeMaker;
import com.sun.tools.javac.util.Context;
import com.sun.tools.javac.util.List;
import com.sun.tools.javac.util.Names;
import net.staticstudios.data.Data;


public class StaticDataJavacPlugin implements Plugin {
    //TODO: properly implement this and match the IntelliJ plugin's behavior
    // note: delegate a lot of behavior to utility classes to avoid generated unnecessary (and less reliable/more complex) code.
    // i.e. AbstractQueryBuilder or something

    @Override
    public String getName() {
        return "StaticDataJavacPlugin";
    }

    @Override
    public void init(JavacTask task, String... args) {
        Context context = ((BasicJavacTask) task).getContext();
        TreeMaker treeMaker = TreeMaker.instance(context);
        Names names = Names.instance(context);
        Trees trees = Trees.instance(task);

        task.addTaskListener(new TaskListener() {
            @Override
            public void finished(TaskEvent e) {
                if (e.getKind() != TaskEvent.Kind.ENTER) return;

                e.getCompilationUnit().accept(new TreeScanner<Void, Void>() {
                    @Override
                    public Void visitClass(ClassTree node, Void unused) {
                        boolean hasDataAnnotation = node.getModifiers().getAnnotations().stream()
                                .anyMatch(a -> {
                                    Tree type = a.getAnnotationType();
                                    if (type instanceof IdentifierTree) { //todo: handle qualified names
                                        return type.toString().equals(Data.class.getSimpleName());
                                    }
                                    return false;
                                });

                        if (!hasDataAnnotation) return super.visitClass(node, unused);

                        JCTree.JCClassDecl classDecl = (JCTree.JCClassDecl) node;

                        boolean hasBuilderClass = classDecl.defs.stream()
                                .anyMatch(def -> def instanceof JCTree.JCClassDecl &&
                                        ((JCTree.JCClassDecl) def).name.toString().equals("Builder"));
                        boolean hasBuilderMethod = classDecl.defs.stream()
                                .anyMatch(def -> def instanceof JCTree.JCMethodDecl &&
                                        ((JCTree.JCMethodDecl) def).name.toString().equals("builder") &&
                                        (((JCTree.JCMethodDecl) def).mods.flags & Flags.STATIC) != 0);

                        if (!hasBuilderClass) {
                            System.err.println("Adding Builder class to " + classDecl.name);
                            JCTree.JCClassDecl builderClass = treeMaker.ClassDef(
                                    treeMaker.Modifiers(Flags.PUBLIC | Flags.STATIC),
                                    names.fromString("Builder"),
                                    List.nil(),
                                    null,
                                    List.nil(),
                                    List.nil()
                            );
                            classDecl.defs = classDecl.defs.append(builderClass);
                        }
                        if (!hasBuilderMethod) {
                            System.err.println("Adding builder() method to " + classDecl.name);
                            JCTree.JCMethodDecl builderMethod = treeMaker.MethodDef(
                                    treeMaker.Modifiers(Flags.PUBLIC | Flags.STATIC),
                                    names.fromString("builder"),
                                    treeMaker.Ident(names.fromString("Builder")),
                                    List.nil(),
                                    List.nil(),
                                    List.nil(),
                                    treeMaker.Block(0, List.of(
                                            treeMaker.Return(
                                                    treeMaker.NewClass(null, List.nil(),
                                                            treeMaker.Ident(names.fromString("Builder")),
                                                            List.nil(), null)
                                            )
                                    )),
                                    null
                            );
                            classDecl.defs = classDecl.defs.append(builderMethod);
                        }
                        return super.visitClass(node, unused);
                    }
                }, null);
            }
        });

    }
}
