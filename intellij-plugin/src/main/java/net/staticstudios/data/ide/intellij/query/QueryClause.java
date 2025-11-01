package net.staticstudios.data.ide.intellij.query;

import com.intellij.psi.*;

import java.util.List;

public interface QueryClause {

    boolean matches(PsiField psiField, boolean nullable);

    String getMethodName(String fieldName);

    List<PsiParameter> getMethodParamTypes(PsiManager manager, PsiType fieldType, PsiElement scope);
}
