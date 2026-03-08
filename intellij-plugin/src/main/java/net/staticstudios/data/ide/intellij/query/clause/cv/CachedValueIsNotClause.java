package net.staticstudios.data.ide.intellij.query.clause.cv;

import com.intellij.psi.*;
import com.intellij.psi.impl.light.LightParameter;
import net.staticstudios.data.ide.intellij.query.QueryClause;

import java.util.List;

public class CachedValueIsNotClause implements QueryClause {

    @Override
    public boolean matches(PsiField psiField, boolean nullable) {
        return true;
    }

    @Override
    public String getMethodName(String fieldName) {
        return fieldName + "IsNot";
    }

    @Override
    public List<PsiParameter> getMethodParamTypes(PsiManager manager, PsiType fieldType, PsiElement scope) {
        return List.of(new LightParameter("value", fieldType, scope));
    }
}
