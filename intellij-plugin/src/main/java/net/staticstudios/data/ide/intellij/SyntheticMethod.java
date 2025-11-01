package net.staticstudios.data.ide.intellij;

import com.intellij.psi.*;
import com.intellij.psi.impl.light.LightMethodBuilder;
import com.intellij.psi.javadoc.PsiDocComment;
import com.intellij.util.IncorrectOperationException;
import org.jetbrains.annotations.NonNls;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.ref.WeakReference;

/**
 * A synthetic method generated for data classes.
 */
public class SyntheticMethod extends LightMethodBuilder implements SyntheticElement {

    private final WeakReference<PsiClass> parentClass;
    private final PsiClass containingClass;
    private final PsiType returnType;
    private final String name;

    public SyntheticMethod(@NotNull PsiClass parentClass, @NotNull PsiClass containingClass, @NotNull String name, PsiType returnType) {
        super(parentClass, parentClass.getLanguage());
        this.name = name;
        this.returnType = returnType;
        this.parentClass = new WeakReference<>(parentClass);
        this.containingClass = containingClass;
        setContainingClass(containingClass);
    }

    @Override
    public @Nullable PsiType getReturnType() {
        return returnType;
    }

    @Override
    public boolean isConstructor() {
        return false;
    }

    @Override
    public boolean isVarArgs() {
        for (PsiParameter parameter : getParameterList().getParameters()) {
            if (parameter.isVarArgs()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public PsiElement setName(@NonNls @NotNull String name) throws IncorrectOperationException {
        return this;
    }

    @Override
    public @Nullable PsiClass getContainingClass() {
        return containingClass;
    }

    @Override
    public boolean hasTypeParameters() {
        return false;
    }

    @Override
    public @Nullable PsiTypeParameterList getTypeParameterList() {
        return null;
    }

    @Override
    public PsiTypeParameter @NotNull [] getTypeParameters() {
        return new PsiTypeParameter[0];
    }

    @Override
    public boolean isValid() {
        PsiClass cls = parentClass.get();
        return cls != null && cls.isValid();
    }

    @Override
    public String toString() {
        return "SyntheticMethod:" + getName();
    }

    @Override
    public @NotNull String getName() {
        return name;
    }

    @Override
    public @NotNull PsiElement getNavigationElement() {
        PsiClass cls = parentClass.get();
        if (cls != null) {
            return cls.getNavigationElement();
        }
        return this;
    }

    @Override
    public PsiElement getParent() {
        return parentClass.get();
    }

    @Override
    public boolean isDeprecated() {
        return false;
    }

    @Override
    public @Nullable PsiDocComment getDocComment() {
        return null;
    }
}