package net.staticstudios.data.compiler.javac.javac;

import com.sun.tools.javac.tree.JCTree;
import com.sun.tools.javac.util.List;

public record SuperClass(String fqn, List<JCTree.JCExpression> superParms, List<JCTree.JCExpression> args) {
}
