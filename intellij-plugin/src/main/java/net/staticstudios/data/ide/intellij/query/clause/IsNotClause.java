package net.staticstudios.data.ide.intellij.query.clause;

import com.intellij.psi.PsiField;
import com.intellij.psi.PsiManager;
import com.intellij.psi.PsiType;
import net.staticstudios.data.ide.intellij.query.QueryClause;

import java.util.List;

public class IsNotClause implements QueryClause {

    @Override
    public boolean matches(PsiField psiField, boolean nullable) {
        return true;
    }

    @Override
    public String getMethodName(String fieldName) {
        return fieldName + "IsNot";
    }

    @Override
    public List<PsiType> getMethodParamTypes(PsiManager manager, PsiType fieldType) {
        return List.of(fieldType);
    }
}
