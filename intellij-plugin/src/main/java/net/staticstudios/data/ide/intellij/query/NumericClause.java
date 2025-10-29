package net.staticstudios.data.ide.intellij.query;

import com.intellij.psi.PsiClassType;
import com.intellij.psi.PsiField;
import net.staticstudios.data.ide.intellij.IntelliJPluginUtils;

public interface NumericClause extends QueryClause {

    @Override
    default boolean matches(PsiField psiField, boolean nullable) {
        if (!(psiField.getType() instanceof PsiClassType psiClassType)) return false;
        return QueryBuilderUtils.isNumeric(IntelliJPluginUtils.getGenericParameter(psiClassType, psiField.getManager()));
    }
}
