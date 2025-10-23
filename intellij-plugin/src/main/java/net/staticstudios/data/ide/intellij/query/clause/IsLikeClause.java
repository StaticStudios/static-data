package net.staticstudios.data.ide.intellij.query.clause;

import com.intellij.psi.PsiField;
import com.intellij.psi.PsiManager;
import com.intellij.psi.PsiType;
import net.staticstudios.data.ide.intellij.Utils;
import net.staticstudios.data.ide.intellij.query.QueryClause;

import java.util.List;

public class IsLikeClause implements QueryClause {

    @Override
    public boolean matches(PsiField psiField, boolean nullable) {
        return Utils.is(psiField.getType(), String.class.getName());
    }

    @Override
    public String getMethodName(String fieldName) {
        return fieldName + "IsLike";
    }

    @Override
    public List<PsiType> getMethodParamTypes(PsiManager manager, PsiType fieldType) {
        return List.of(fieldType);
    }
}
