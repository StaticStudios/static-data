package net.staticstudios.data.ide.intellij.query.clause.cv;

import com.intellij.psi.*;
import net.staticstudios.data.ide.intellij.query.QueryClause;

import java.util.List;

public class CachedValueIsNotNullClause implements QueryClause {

    @Override
    public boolean matches(PsiField psiField, boolean nullable) {
        return true;
    }

    @Override
    public String getMethodName(String fieldName) {
        return fieldName + "IsNotNull";
    }

    @Override
    public List<PsiParameter> getMethodParamTypes(PsiManager manager, PsiType fieldType, PsiElement scope) {
        return List.of();
    }
}
