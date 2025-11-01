package net.staticstudios.data.ide.intellij.query.clause;

import com.google.common.base.Preconditions;
import com.intellij.psi.*;
import com.intellij.psi.impl.light.LightParameter;
import com.intellij.psi.search.GlobalSearchScope;
import net.staticstudios.data.ide.intellij.query.QueryClause;

import java.util.List;

public class IsInCollectionClause implements QueryClause {

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
        JavaPsiFacade facade = JavaPsiFacade.getInstance(manager.getProject());
        PsiClass collectionType = facade.findClass("java.util.Collection", GlobalSearchScope.allScope(manager.getProject()));
        Preconditions.checkNotNull(collectionType, "Could not find java.util.Collection class");
        PsiTypeParameter[] typeParameters = collectionType.getTypeParameters();
        Preconditions.checkState(typeParameters.length == 1, "Expected Collection to have one type parameter");
        PsiSubstitutor substitutor = PsiSubstitutor.EMPTY;
        substitutor = substitutor.put(typeParameters[0], fieldType);
        return List.of(new LightParameter("values", facade.getElementFactory().createType(collectionType, substitutor), scope));
    }
}
