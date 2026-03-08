package net.staticstudios.data.ide.intellij;

import com.intellij.psi.*;
import com.intellij.psi.impl.light.LightMethodBuilder;
import com.intellij.psi.javadoc.PsiDocComment;
import com.intellij.util.IncorrectOperationException;
import org.jetbrains.annotations.NonNls;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.ref.WeakReference;
import java.util.Objects;

/**
 * A synthetic method generated for data classes.
 */
public class SyntheticMethod extends LightMethodBuilder implements SyntheticElement {

    private final WeakReference<PsiClass> parentClass;
    private final PsiClass containingClass;
    private final PsiType returnType;
    private final String name;
    private @Nullable PsiField sourceField;

    public SyntheticMethod(@NotNull PsiClass parentClass, @NotNull PsiClass containingClass, @NotNull String name, PsiType returnType) {
        super(parentClass, parentClass.getLanguage());
        this.name = name;
        this.returnType = returnType;
        this.parentClass = new WeakReference<>(parentClass);
        this.containingClass = containingClass;
        setContainingClass(containingClass);
    }

    public void setSourceField(@Nullable PsiField sourceField) {
        this.sourceField = sourceField;
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
        if (sourceField != null && sourceField.isValid()) {
            return sourceField.getNavigationElement();
        }
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SyntheticMethod other)) return false;
        if (!Objects.equals(name, other.name)) return false;
        if (!Objects.equals(containingClass, other.containingClass)) return false;
        PsiParameter[] params = getParameterList().getParameters();
        PsiParameter[] otherParams = other.getParameterList().getParameters();
        if (params.length != otherParams.length) return false;
        for (int i = 0; i < params.length; i++) {
            if (!Objects.equals(params[i].getName(), otherParams[i].getName())) return false;
            if (!Objects.equals(params[i].getType().getCanonicalText(), otherParams[i].getType().getCanonicalText()))
                return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, containingClass);
    }
}