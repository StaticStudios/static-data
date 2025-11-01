package net.staticstudios.data.ide.intellij.query.clause;

import com.intellij.psi.*;
import net.staticstudios.data.ide.intellij.query.QueryClause;

import java.util.List;

public class IsNotNullClause implements QueryClause {

    @Override
    public boolean matches(PsiField psiField, boolean nullable) {
        return nullable;
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
