package net.staticstudios.data.ide.intellij.query.clause;

import com.intellij.psi.*;
import com.intellij.psi.impl.light.LightParameter;
import net.staticstudios.data.ide.intellij.IntelliJPluginUtils;
import net.staticstudios.data.ide.intellij.query.QueryClause;

import java.util.List;

public class IsNotLikeClause implements QueryClause {

    @Override
    public boolean matches(PsiField psiField, boolean nullable) {
        return IntelliJPluginUtils.genericTypeIs(psiField.getType(), String.class.getName());
    }

    @Override
    public String getMethodName(String fieldName) {
        return fieldName + "IsNotLike";
    }

    @Override
    public List<PsiParameter> getMethodParamTypes(PsiManager manager, PsiType fieldType, PsiElement scope) {
        return List.of(new LightParameter("pattern", fieldType, scope));
    }
}
