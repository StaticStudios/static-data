package net.staticstudios.data.compiler.javac;

import com.sun.source.util.Trees;
import com.sun.tools.javac.tree.JCTree;
import com.sun.tools.javac.util.Context;
import net.staticstudios.data.Data;
import net.staticstudios.data.compiler.javac.javac.ParsedPersistentValue;
import net.staticstudios.data.compiler.javac.javac.ParsedReference;
import net.staticstudios.data.compiler.javac.util.TypeUtils;

import javax.lang.model.element.TypeElement;
import java.util.Collection;

public record ProcessorContext(
        Context context,
        Trees trees,
        TypeUtils typeUtils,
        Data dataAnnotation,
        TypeElement dataClassElement,
        JCTree.JCClassDecl dataClassDecl,
        Collection<ParsedPersistentValue> persistentValues,
        Collection<ParsedReference> references
) {
}
