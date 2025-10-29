package net.staticstudios.data.compiler.javac;

import com.sun.tools.javac.tree.JCTree;
import com.sun.tools.javac.tree.TreeMaker;
import com.sun.tools.javac.util.Names;

public class QueryBuilderProcessor extends AbstractBuilderProcessor {


    public QueryBuilderProcessor(JCTree.JCCompilationUnit compilationUnit, TreeMaker treeMaker, Names names, JCTree.JCClassDecl dataClassDecl, ParsedDataAnnotation dataAnnotation) {
        super(compilationUnit, treeMaker, names, dataClassDecl, dataAnnotation, "QueryBuilder", "query");
    }

    @Override
    protected void addImports() {
        JavaCPluginUtils.importClass(compilationUnit, treeMaker, names, "net.staticstudios.data.util", "ValueUtils");

    }

    @Override
    protected void process() {
        //todo: impl
    }
}
