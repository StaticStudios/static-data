package net.staticstudios.data.ide.intellij.query.clause;

import com.intellij.psi.PsiManager;
import com.intellij.psi.PsiType;
import net.staticstudios.data.ide.intellij.query.NumericClause;

import java.util.List;

public class IsLessThanOrEqualToClause implements NumericClause {

    @Override
    public String getMethodName(String fieldName) {
        return fieldName + "IsLessThanOrEqualTo";
    }

    @Override
    public List<PsiType> getMethodParamTypes(PsiManager manager, PsiType fieldType) {
        return List.of(fieldType);
    }
}
