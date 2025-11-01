package net.staticstudios.data.compiler.javac;

import com.sun.tools.javac.tree.JCTree;
import com.sun.tools.javac.util.List;

public record SuperClass(String simpleName, List<JCTree.JCExpression> superParms, List<JCTree.JCExpression> args) {
}
