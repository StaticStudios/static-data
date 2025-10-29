package net.staticstudios.data.compiler.javac;

import com.sun.tools.javac.code.Attribute;
import com.sun.tools.javac.code.Flags;
import com.sun.tools.javac.code.Symbol;
import com.sun.tools.javac.code.Type;
import com.sun.tools.javac.tree.JCTree;
import com.sun.tools.javac.tree.TreeMaker;
import com.sun.tools.javac.util.List;
import com.sun.tools.javac.util.Name;
import com.sun.tools.javac.util.Names;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collection;

public class JavaCPluginUtils {
    public static boolean isAnnotation(@NotNull JCTree.JCAnnotation annotation, @NotNull String targetFqn) {
        JCTree annotationType = annotation.getAnnotationType();
        return isFQN(annotationType, targetFqn);
    }

    public static boolean isAnnotation(@NotNull Attribute.Compound annotation, @NotNull String targetFqn) {
        Symbol.TypeSymbol typeSymbol = annotation.type.tsym;
        if (typeSymbol == null) {
            return false;
        }
        Name qualifiedName = typeSymbol.getQualifiedName();
        return qualifiedName.contentEquals(targetFqn);
    }

    public static boolean isFQN(@Nullable JCTree tree, @NotNull String targetFqn) {
        if (tree == null) {
            return false;
        }
        Type type = tree.type;
        if (type == null) {
            return false;
        }
        String fqn = type.toString();
        return fqn.equals(targetFqn);
    }

    public static boolean isFQN(@Nullable Symbol.VarSymbol varSymbol, @NotNull String targetFqn) {
        if (varSymbol == null) {
            return false;
        }

        Symbol typeSymbol = varSymbol.type != null ? varSymbol.type.tsym : null;
        if (typeSymbol == null) {
            return false;
        }

        return typeSymbol.getQualifiedName().contentEquals(targetFqn);
    }

    public static @Nullable JCTree.JCAnnotation extractAnnotation(JCTree.JCClassDecl classDecl, String targetFqn) {
        for (JCTree.JCAnnotation annotation : classDecl.getModifiers().getAnnotations()) {
            if (isAnnotation(annotation, targetFqn)) {
                return annotation;
            }
        }
        return null;
    }

    /**
     * Get a string annotation value, treating empty strings as null
     */
    public static @Nullable String getStringAnnotationValue(@NotNull JCTree.JCAnnotation annotation, @NotNull String key) {
        String value = getAnnotationValue(annotation, String.class, key);
        if (value != null && !value.isEmpty()) {
            return value;
        }
        return null; // Treat empty strings as null
    }

    public static <T> @Nullable T getAnnotationValue(@NotNull JCTree.JCAnnotation annotation, @NotNull Class<T> type, @NotNull String key) {
        List<JCTree.JCExpression> args = annotation.args;
        if (args == null) {
            return null;
        }
        for (JCTree.JCExpression arg : args) {
            if (arg instanceof JCTree.JCAssign assign) {
                String propertyName = assign.lhs.toString();
                if (propertyName.equals(key)) {
                    if (assign.rhs instanceof JCTree.JCLiteral rhs) {
                        if (type.isInstance(rhs.value)) {
                            return type.cast(rhs.value);
                        }
                    } else {
                        throw new UnsupportedOperationException("Cannot handle non-literal annotation values yet");
                    }

                    return null;
                }
            } else if ("value".equals(key)) {
                if (arg instanceof JCTree.JCLiteral literal) {
                    if (type.isInstance(literal.value)) {
                        return type.cast(literal.value);
                    }
                } else {
                    throw new UnsupportedOperationException("Cannot handle non-literal annotation values yet");
                }

                return null;
            }
        }
        return null;
    }

    /**
     * Get a string annotation value, treating empty strings as null
     */
    public static @Nullable String getStringAnnotationValue(@NotNull Attribute.Compound annotation, @NotNull String key) {
        String value = getAnnotationValue(annotation, String.class, key);
        if (value != null && !value.isEmpty()) {
            return value;
        }
        return null; // Treat empty strings as null
    }

    public static <T> @Nullable T getAnnotationValue(@NotNull Attribute.Compound annotation,
                                                     @NotNull Class<T> type,
                                                     @NotNull String key) {
        for (var pair : annotation.values) {
            String elementName = pair.fst.getSimpleName().toString();
            if (!elementName.equals(key)) continue;

            Object constValue = extractAnnotationValue(pair.snd);
            if (type.isInstance(constValue)) {
                return type.cast(constValue);
            }
            return null;
        }

        if ("value".equals(key) && annotation.values.isEmpty()) {
            Object defaultVal = getDefaultAnnotationValue(annotation, key);
            if (type.isInstance(defaultVal)) {
                return type.cast(defaultVal);
            }
        }

        return null;
    }

    public static @Nullable String getStringAnnotationValue(java.util.List<Attribute.Compound> annotations,
                                                            @NotNull String targetFqn,
                                                            @NotNull String key) {
        String value = getAnnotationValue(annotations, String.class, targetFqn, key);
        if (value != null && !value.isEmpty()) {
            return value;
        }
        return null; // Treat empty strings as null
    }

    public static <T> @Nullable T getAnnotationValue(java.util.List<Attribute.Compound> annotations,
                                                     @NotNull Class<T> type,
                                                     @NotNull String targetFqn,
                                                     @NotNull String key) {
        for (Attribute.Compound annotation : annotations) {
            if (isAnnotation(annotation, targetFqn)) {
                return getAnnotationValue(annotation, type, key);
            }
        }
        return null;
    }

    public static JCTree.JCExpression makeFqnIdent(@NotNull TreeMaker treeMaker, @NotNull Names names, @NotNull String fqn) {
        String[] parts = fqn.split("\\.");
        JCTree.JCExpression expression = treeMaker.Ident(names.fromString(parts[0]));
        for (int i = 1; i < parts.length; i++) {
            expression = treeMaker.Select(expression, names.fromString(parts[i]));
        }
        return expression;
    }

    public static void importClass(@NotNull JCTree.JCCompilationUnit compilationUnit, @NotNull TreeMaker treeMaker, @NotNull Names names, String packageName, String className) {
        // Build a package expression that supports dot-qualified names (uses makeFqnIdent)
        JCTree.JCExpression pkgExpr = makeFqnIdent(treeMaker, names, packageName);
        JCTree.JCImport jcImport = treeMaker.Import(
                treeMaker.Select(
                        pkgExpr,
                        names.fromString(className)
                ),
                false
        );

        // Don't add duplicate imports: compare the string form of existing imports
        for (JCTree def : compilationUnit.defs) {
            if (def instanceof JCTree.JCImport) {
                if (def.toString().equals(jcImport.toString())) {
                    return; // already imported
                }
            }
        }

        // Insert the import before the first top-level class declaration. This keeps
        // imports after package/imports and avoids placing them before the package.
        List<JCTree> oldDefs = compilationUnit.defs;
        List<JCTree> newDefs = List.nil();
        boolean inserted = false;
        for (JCTree def : oldDefs) {
            if (!inserted && def instanceof JCTree.JCClassDecl) {
                newDefs = newDefs.append(jcImport);
                inserted = true;
            }
            newDefs = newDefs.append(def);
        }
        if (!inserted) {
            // no class declarations found; append the import at the end
            newDefs = newDefs.append(jcImport);
        }
        compilationUnit.defs = newDefs;
    }

    public static void generatePrivateStaticField(
            @NotNull TreeMaker treeMaker,
            @NotNull Names names,
            @NotNull JCTree.JCClassDecl classDecl,
            @NotNull String name,
            @NotNull JCTree.JCExpression type,
            @Nullable JCTree.JCExpression init
    ) {
        JCTree.JCVariableDecl fieldDef = treeMaker.VarDef(
                treeMaker.Modifiers(Flags.PRIVATE | Flags.STATIC | Flags.FINAL),
                names.fromString(name),
                type,
                init
        );
        classDecl.defs = classDecl.defs.append(fieldDef);
    }

    public static void generatePrivateMemberField(
            @NotNull TreeMaker treeMaker,
            @NotNull Names names,
            @NotNull JCTree.JCClassDecl classDecl,
            @NotNull String name,
            @NotNull JCTree.JCExpression type,
            @Nullable JCTree.JCExpression init
    ) {
        JCTree.JCVariableDecl fieldDef = treeMaker.VarDef(
                treeMaker.Modifiers(Flags.PRIVATE),
                names.fromString(name),
                type,
                init
        );
        classDecl.defs = classDecl.defs.append(fieldDef);
    }

    private static @Nullable Object extractAnnotationValue(@NotNull Attribute attribute) {
        return switch (attribute) {
            case Attribute.Constant c -> c.getValue();
            case Attribute.Enum e -> e.value.toString();
            case Attribute.Class c -> c.classType.tsym.getQualifiedName().toString();
            default -> null;
        };
    }

    private static @Nullable Object getDefaultAnnotationValue(@NotNull Attribute.Compound annotation, @NotNull String key) {
        Symbol.TypeSymbol typeSymbol = annotation.type.tsym;
        if (!(typeSymbol instanceof Symbol.ClassSymbol classSym)) {
            return null;
        }

        for (Symbol member : classSym.members().getSymbols()) {
            if (member instanceof Symbol.MethodSymbol method) {
                Name name = method.name;
                if (name != null && name.contentEquals(key)) {
                    Attribute defaultValue = method.getDefaultValue();
                    if (defaultValue != null) {
                        return extractAnnotationValue(defaultValue);
                    }
                }
            }
        }
        return null;
    }

    public static Collection<Symbol.VarSymbol> getFields(@NotNull JCTree.JCClassDecl classDecl, @NotNull String targetFqn) {
        java.util.List<Symbol.VarSymbol> fieldList = new ArrayList<>();
        getFields(classDecl.sym, targetFqn, fieldList);
        return fieldList;
    }

    private static void getFields(@NotNull Symbol.ClassSymbol classSymbol, @NotNull String targetFqn, Collection<Symbol.VarSymbol> fields) {
        for (Symbol symbol : classSymbol.getEnclosedElements()) {
            if (!(symbol instanceof Symbol.VarSymbol varSymbol)) {
                continue;
            }

            if (isFQN(varSymbol, targetFqn)) {
                fields.add(varSymbol);
            }
        }

        Type superType = classSymbol.getSuperclass();
        if (superType != null && superType.tsym instanceof Symbol.ClassSymbol superClassSymbol) {
            getFields(superClassSymbol, targetFqn, fields);
        }
    }

    public static @Nullable JCTree.JCExpression getGenericTypeExpression(
            @NotNull TreeMaker treeMaker,
            @NotNull Names names,
            @NotNull Symbol.VarSymbol varSymbol,
            int index
    ) {
        Type varType = varSymbol.type;
        if (!(varType instanceof Type.ClassType classType)) {
            return null;
        }
        com.sun.tools.javac.util.List<Type> typeArgs = classType.getTypeArguments();
        if (typeArgs == null || typeArgs.isEmpty() || index < 0 || index >= typeArgs.size()) {
            return null;
        }
        return typeToExpression(treeMaker, names, typeArgs.get(index));
    }

    private static @Nullable JCTree.JCExpression typeToExpression(@NotNull TreeMaker treeMaker,
                                                                  @NotNull Names names,
                                                                  @Nullable Type type) {
        if (type == null) return null;

        // Parameterized / class types
        if (type.tsym != null) {
            String qn = type.tsym.getQualifiedName().toString();
            JCTree.JCExpression base = makeFqnIdent(treeMaker, names, qn);

            if (type instanceof Type.ClassType classType) {
                com.sun.tools.javac.util.List<Type> args = classType.getTypeArguments();
                if (args != null && !args.isEmpty()) {
                    com.sun.tools.javac.util.List<JCTree.JCExpression> jcArgs = List.nil();
                    for (Type ta : args) {
                        JCTree.JCExpression expr = typeToExpression(treeMaker, names, ta);
                        jcArgs = jcArgs.append(expr != null ? expr : treeMaker.Ident(names.fromString(ta.toString())));
                    }
                    return treeMaker.TypeApply(base, jcArgs);
                }
            }
            return base;
        }

        // Array types
        if (type instanceof Type.ArrayType arr) {
            JCTree.JCExpression elem = typeToExpression(treeMaker, names, arr.elemtype);
            return elem != null ? treeMaker.TypeArray(elem) : null;
        }

        // Fallback to a simple identifier (covers type variables / wildcards minimally)
        return treeMaker.Ident(names.fromString(type.toString()));
    }
}


