package net.staticstudios.data.ide.intellij.query;

import com.intellij.psi.PsiField;
import com.intellij.psi.PsiManager;
import com.intellij.psi.PsiType;

import java.util.List;

public interface QueryClause {

    boolean matches(PsiField psiField, boolean nullable);

    String getMethodName(String fieldName);

    List<PsiType> getMethodParamTypes(PsiManager manager, PsiType fieldType);
}
