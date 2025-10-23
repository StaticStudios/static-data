package net.staticstudios.data.ide.intellij;

import com.intellij.lang.java.JavaLanguage;
import com.intellij.psi.*;
import com.intellij.psi.impl.PsiClassImplUtil;
import com.intellij.psi.impl.light.LightModifierList;
import com.intellij.psi.impl.light.LightPsiClassBase;
import com.intellij.psi.search.GlobalSearchScope;
import com.intellij.psi.search.SearchScope;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;

/**
 * A synthetic builder class generated for data classes.
 * "Builder" refers to the builder pattern, not to be confused with other uses of the term "builder".
 */
public class SyntheticBuilderClass extends LightPsiClassBase {

    private final WeakReference<PsiClass> parentClass;
    private final List<PsiMethod> methods = new ArrayList<>();
    private final LightModifierList modifierList;

    public SyntheticBuilderClass(@NotNull PsiClass parentClass, String suffix) {
        super(parentClass, parentClass.getName() + suffix);
        this.parentClass = new WeakReference<>(parentClass);
        this.modifierList = new LightModifierList(getManager(), JavaLanguage.INSTANCE, PsiModifier.PUBLIC, PsiModifier.STATIC, PsiModifier.FINAL);
    }

    public void addMethod(PsiMethod method) {
        methods.add(method);
    }

    @Override
    public @NotNull GlobalSearchScope getResolveScope() {
        PsiClass parentClass = this.parentClass.get();
        if (parentClass != null) {
            return parentClass.getResolveScope();
        }
        return super.getResolveScope();
    }

    @Override
    public @NotNull SearchScope getUseScope() {
        PsiClass parentClass = this.parentClass.get();
        if (parentClass != null) {
            return parentClass.getUseScope();
        }
        return super.getUseScope();
    }

    @Override
    public @NotNull PsiModifierList getModifierList() {
        return modifierList;
    }

    @Override
    public @Nullable PsiReferenceList getExtendsList() {
        return null;
    }

    @Override
    public @Nullable PsiReferenceList getImplementsList() {
        return null;
    }

    @Override
    public PsiField @NotNull [] getFields() {
        return PsiField.EMPTY_ARRAY;
    }

    @Override
    public PsiMethod @NotNull [] getMethods() {
        return methods.toArray(PsiMethod.EMPTY_ARRAY);
    }

    @Override
    public PsiClass @NotNull [] getInnerClasses() {
        return PsiClass.EMPTY_ARRAY;
    }

    @Override
    public PsiClassInitializer @NotNull [] getInitializers() {
        return PsiClassInitializer.EMPTY_ARRAY;
    }

    @Override
    public PsiElement getScope() {
        return null;
    }

    @Override
    public @Nullable PsiClass getContainingClass() {
        return parentClass.get();
    }

    @Override
    public @Nullable PsiTypeParameterList getTypeParameterList() {
        return null;
    }

    @Override
    public boolean isValid() {
        PsiClass parentClass = this.parentClass.get();
        return parentClass != null && parentClass.isValid();
    }

    @Override
    public PsiFile getContainingFile() {
        PsiClass parentClass = this.parentClass.get();
        if (parentClass != null) {
            return parentClass.getContainingFile();
        }
        return null;
    }

    @Override
    public @NotNull PsiElement getNavigationElement() {
        PsiClass parentClass = this.parentClass.get();
        if (parentClass != null) {
            return parentClass.getNavigationElement();
        }
        return this;
    }

    @Override
    public PsiElement getParent() {
        return null; // Note: when returning parentClass.get(), there are issues finding the class, for some reason. So don't return it.
    }

    @Override
    public boolean isEquivalentTo(PsiElement another) {
        return PsiClassImplUtil.isClassEquivalentTo(this, another);
    }
}