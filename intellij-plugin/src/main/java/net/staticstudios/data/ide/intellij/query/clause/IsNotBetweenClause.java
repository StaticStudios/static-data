package net.staticstudios.data.ide.intellij.query.clause;

import com.intellij.psi.PsiElement;
import com.intellij.psi.PsiManager;
import com.intellij.psi.PsiParameter;
import com.intellij.psi.PsiType;
import com.intellij.psi.impl.light.LightParameter;
import net.staticstudios.data.ide.intellij.query.NumericClause;

import java.util.List;

public class IsNotBetweenClause implements NumericClause {

    @Override
    public String getMethodName(String fieldName) {
        return fieldName + "IsNotBetween";
    }

    @Override
    public List<PsiParameter> getMethodParamTypes(PsiManager manager, PsiType fieldType, PsiElement scope) {
        return List.of(
                new LightParameter("min", fieldType, scope),
                new LightParameter("max", fieldType, scope)
        );
    }
}
