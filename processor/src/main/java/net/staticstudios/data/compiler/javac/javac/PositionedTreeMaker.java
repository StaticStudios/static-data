package net.staticstudios.data.compiler.javac.javac;

import com.sun.tools.javac.code.Symbol;
import com.sun.tools.javac.code.TypeTag;
import com.sun.tools.javac.tree.JCTree;
import com.sun.tools.javac.tree.TreeMaker;
import com.sun.tools.javac.util.Context;
import com.sun.tools.javac.util.List;
import com.sun.tools.javac.util.Name;
import com.sun.tools.javac.util.Names;

public class PositionedTreeMaker {
    private final TreeMaker treeMaker;
    private final Names names;
    private final int pos;

    public PositionedTreeMaker(Context context, int pos) {
        this.treeMaker = TreeMaker.instance(context);
        this.names = Names.instance(context);
        this.pos = pos;
    }

    public JCTree.JCExpression chainDots(String... namesArray) {
        treeMaker.at(pos);
        JCTree.JCExpression expr = treeMaker.Ident(this.names.fromString(namesArray[0]));

        for (int i = 1; i < namesArray.length; i++) {
            treeMaker.at(pos);
            expr = treeMaker.Select(expr, this.names.fromString(namesArray[i]));
        }
        return expr;
    }

    public Name toName(String s) {
        return names.fromString(s);
    }

    public JCTree.JCFieldAccess Select(JCTree.JCExpression expr, Name name) {
        return treeMaker.at(pos).Select(expr, name);
    }

    public JCTree.JCIdent Ident(Name name) {
        return treeMaker.at(pos).Ident(name);
    }

    public JCTree.JCLiteral Literal(Object value) {
        return treeMaker.at(pos).Literal(value);
    }

    public JCTree.JCLiteral Literal(TypeTag tag, Object value) {
        return treeMaker.at(pos).Literal(tag, value);
    }

    public JCTree.JCClassDecl ClassDef(JCTree.JCModifiers mods, Name name, List<JCTree.JCTypeParameter> typarams, JCTree.JCExpression extending, List<JCTree.JCExpression> implementing, List<JCTree> defs) {
        return treeMaker.at(pos).ClassDef(mods, name, typarams, extending, implementing, defs);
    }

    public JCTree.JCMethodDecl MethodDef(JCTree.JCModifiers mods, Name name, JCTree.JCExpression resType, List<JCTree.JCTypeParameter> typarams, List<JCTree.JCVariableDecl> params, List<JCTree.JCExpression> thrown, JCTree.JCBlock body, JCTree.JCExpression defaultValue) {
        return treeMaker.at(pos).MethodDef(mods, name, resType, typarams, params, thrown, body, defaultValue);
    }

    public JCTree.JCVariableDecl VarDef(JCTree.JCModifiers mods, Name name, JCTree.JCExpression vartype, JCTree.JCExpression init) {
        return treeMaker.at(pos).VarDef(mods, name, vartype, init);
    }

    public JCTree.JCModifiers Modifiers(long flags, List<JCTree.JCAnnotation> annotations) {
        return treeMaker.at(pos).Modifiers(flags, annotations);
    }

    /**
     * Creates Modifiers with no annotations.
     */
    public JCTree.JCModifiers Modifiers(long flags) {
        return treeMaker.at(pos).Modifiers(flags, List.nil());
    }

    public JCTree.JCBlock Block(long flags, List<JCTree.JCStatement> stats) {
        return treeMaker.at(pos).Block(flags, stats);
    }

    public JCTree.JCMethodInvocation Apply(List<JCTree.JCExpression> typeargs, JCTree.JCExpression fn, List<JCTree.JCExpression> args) {
        return treeMaker.at(pos).Apply(typeargs, fn, args);
    }

    public JCTree.JCNewClass NewClass(JCTree.JCExpression encl, List<JCTree.JCExpression> typeargs, JCTree.JCExpression clazz, List<JCTree.JCExpression> args, JCTree.JCClassDecl def) {
        return treeMaker.at(pos).NewClass(encl, typeargs, clazz, args, def);
    }

    public JCTree.JCAssign Assign(JCTree.JCExpression lhs, JCTree.JCExpression rhs) {
        return treeMaker.at(pos).Assign(lhs, rhs);
    }

    public JCTree.JCExpressionStatement Exec(JCTree.JCExpression expr) {
        return treeMaker.at(pos).Exec(expr);
    }

    public JCTree.JCReturn Return(JCTree.JCExpression expr) {
        return treeMaker.at(pos).Return(expr);
    }

    public JCTree.JCTypeApply TypeApply(JCTree.JCExpression clazz, List<JCTree.JCExpression> args) {
        return treeMaker.at(pos).TypeApply(clazz, args);
    }

    public JCTree.JCArrayTypeTree TypeArray(JCTree.JCExpression elemtype) {
        return treeMaker.at(pos).TypeArray(elemtype);
    }

    public JCTree.JCTypeParameter TypeParameter(Name name, List<JCTree.JCExpression> bounds) {
        return treeMaker.at(pos).TypeParameter(name, bounds);
    }

    public JCTree.JCImport Import(JCTree.JCFieldAccess qualid, boolean staticImport) {
        return treeMaker.at(pos).Import(qualid, staticImport);
    }

    public JCTree.JCTypeCast TypeCast(JCTree.JCExpression type, JCTree.JCExpression expr) {
        return treeMaker.at(pos).TypeCast(type, expr);
    }

    public JCTree.JCTry Try(JCTree.JCBlock body, List<JCTree.JCCatch> catchers, JCTree.JCBlock finalizer) {
        return treeMaker.at(pos).Try(body, catchers, finalizer);
    }

    public JCTree.JCCatch Catch(JCTree.JCVariableDecl param, JCTree.JCBlock body) {
        return treeMaker.at(pos).Catch(param, body);
    }

    public JCTree.JCAssert Assert(JCTree.JCExpression cond, JCTree.JCExpression detail) {
        return treeMaker.at(pos).Assert(cond, detail);
    }

    public JCTree.JCBinary Binary(JCTree.Tag tag, JCTree.JCExpression lhs, JCTree.JCExpression rhs) {
        return treeMaker.at(pos).Binary(tag, lhs, rhs);
    }

    public JCTree.JCUnary Unary(JCTree.Tag tag, JCTree.JCExpression arg) {
        return treeMaker.at(pos).Unary(tag, arg);
    }

    public JCTree.JCNewArray NewArray(JCTree.JCExpression elemtype, List<JCTree.JCExpression> dims, List<JCTree.JCExpression> elems) {
        return treeMaker.at(pos).NewArray(elemtype, dims, elems);
    }

    public JCTree.JCPrimitiveTypeTree TypeIdent(TypeTag tag) {
        return treeMaker.at(pos).TypeIdent(tag);
    }

    public JCTree.JCArrayAccess Indexed(JCTree.JCExpression array, JCTree.JCExpression index) {
        return treeMaker.at(pos).Indexed(array, index);
    }

    public JCTree.JCArrayAccess Indexed(Symbol v, JCTree.JCExpression index) {
        return treeMaker.at(pos).Indexed(v, index);
    }

    public JCTree.JCIf If(JCTree.JCExpression cond, JCTree.JCStatement thenpart, JCTree.JCStatement elsepart) {
        return treeMaker.at(pos).If(cond, thenpart, elsepart);
    }

    public JCTree.JCForLoop ForLoop(List<JCTree.JCStatement> init, JCTree.JCExpression cond, List<JCTree.JCExpressionStatement> step, JCTree.JCStatement body) {
        return treeMaker.at(pos).ForLoop(init, cond, step, body);
    }

    public JCTree.JCEnhancedForLoop ForeachLoop(JCTree.JCVariableDecl var, JCTree.JCExpression expr, JCTree.JCBlock body) {
        return treeMaker.at(pos).ForeachLoop(var, expr, body);
    }

    public JCTree.JCConditional Conditional(JCTree.JCExpression cond, JCTree.JCExpression truepart, JCTree.JCExpression falsepart) {
        return treeMaker.at(pos).Conditional(cond, truepart, falsepart);
    }

    public JCTree.JCAnnotation Annotation(JCTree.JCExpression annotationType, List<JCTree.JCExpression> args) {
        return treeMaker.at(pos).Annotation(annotationType, args);
    }

    public JCTree.JCTypeUnion TypeUnion(List<JCTree.JCExpression> alternatives) {
        return treeMaker.at(pos).TypeUnion(alternatives);
    }

}
