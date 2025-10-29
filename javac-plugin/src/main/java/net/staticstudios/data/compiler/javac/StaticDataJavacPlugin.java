package net.staticstudios.data.compiler.javac;

import com.sun.source.tree.ClassTree;
import com.sun.source.util.*;
import com.sun.tools.javac.api.BasicJavacTask;
import com.sun.tools.javac.tree.JCTree;
import com.sun.tools.javac.tree.TreeMaker;
import com.sun.tools.javac.util.Context;
import com.sun.tools.javac.util.Names;
import net.staticstudios.data.utils.Constants;


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

        task.addTaskListener(new TaskListener() {
            @Override
            public void finished(TaskEvent e) {
                if (e.getKind() != TaskEvent.Kind.ENTER) return;

                e.getCompilationUnit().accept(new TreeScanner<Void, Void>() {
                    @Override
                    public Void visitClass(ClassTree node, Void unused) {
                        boolean hasDataAnnotation = node.getModifiers().getAnnotations().stream()
                                .anyMatch(a -> JavaCPluginUtils.isAnnotation((JCTree.JCAnnotation) a, Constants.DATA_ANNOTATION_FQN));

                        if (hasDataAnnotation) {
                            JCTree.JCClassDecl classDecl = (JCTree.JCClassDecl) node;

                            if (!BuilderProcessor.hasProcessed(classDecl)) {
                                ParsedDataAnnotation dataAnnotation = ParsedDataAnnotation.extract(classDecl);
                                new BuilderProcessor((JCTree.JCCompilationUnit) e.getCompilationUnit(), treeMaker, names, classDecl, dataAnnotation).runProcessor();
                                new QueryBuilderProcessor((JCTree.JCCompilationUnit) e.getCompilationUnit(), treeMaker, names, classDecl, dataAnnotation).runProcessor();
                            }
                        }

                        return super.visitClass(node, unused);
                    }
                }, null);
            }
        });
    }
}
