package net.staticstudios.data.ide.intellij.query.clause;

import com.intellij.psi.PsiField;
import com.intellij.psi.PsiManager;
import com.intellij.psi.PsiType;
import net.staticstudios.data.ide.intellij.query.QueryClause;

import java.util.Collections;
import java.util.List;

public class IsNullClause implements QueryClause {

    @Override
    public boolean matches(PsiField psiField, boolean nullable) {
        return nullable;
    }

    @Override
    public String getMethodName(String fieldName) {
        return fieldName + "IsNull";
    }

    @Override
    public List<PsiType> getMethodParamTypes(PsiManager manager, PsiType fieldType) {
        return Collections.emptyList();
    }
}
