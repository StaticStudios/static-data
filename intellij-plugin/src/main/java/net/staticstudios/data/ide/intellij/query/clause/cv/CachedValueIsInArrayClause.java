package net.staticstudios.data.ide.intellij.query.clause.cv;

import com.google.common.base.Preconditions;
import com.intellij.lang.java.JavaLanguage;
import com.intellij.psi.*;
import com.intellij.psi.impl.light.LightParameter;
import com.intellij.psi.search.GlobalSearchScope;
import com.intellij.util.IncorrectOperationException;
import net.staticstudios.data.ide.intellij.query.QueryClause;

import java.util.List;

public class CachedValueIsInArrayClause implements QueryClause {

    @Override
    public boolean matches(PsiField psiField, boolean nullable) {
        return true;
    }

    @Override
    public String getMethodName(String fieldName) {
        return fieldName + "IsIn";
    }

    @Override
    public List<PsiParameter> getMethodParamTypes(PsiManager manager, PsiType fieldType, PsiElement scope) {
        PsiElementFactory factory = JavaPsiFacade.getElementFactory(manager.getProject());

        String arrayTypeName = fieldType.getPresentableText();
        String dummyMethodText = "void dummy(" + arrayTypeName + "... values) {}";

        try {
            PsiMethod dummyMethod = factory.createMethodFromText(dummyMethodText, scope);
            PsiParameter varargsParam = dummyMethod.getParameterList().getParameters()[0];
            LightParameter lightParam = new LightParameter(
                    varargsParam.getName(),
                    varargsParam.getType(),
                    scope,
                    JavaLanguage.INSTANCE,
                    true
            );

            return List.of(lightParam);

        } catch (IncorrectOperationException e) {
            return List.of(new LightParameter("values", fieldType.createArrayType(), scope, JavaLanguage.INSTANCE, true));
        }
    }
}
