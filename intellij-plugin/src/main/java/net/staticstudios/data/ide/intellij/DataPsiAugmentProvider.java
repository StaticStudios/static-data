package net.staticstudios.data.ide.intellij;

import com.intellij.openapi.util.Key;
import com.intellij.openapi.util.text.StringUtil;
import com.intellij.psi.*;
import com.intellij.psi.augment.PsiAugmentProvider;
import com.intellij.psi.search.GlobalSearchScope;
import com.intellij.psi.util.CachedValue;
import com.intellij.psi.util.CachedValueProvider;
import com.intellij.psi.util.CachedValuesManager;
import net.staticstudios.data.ide.intellij.query.QueryBuilderUtils;
import net.staticstudios.data.ide.intellij.query.QueryClause;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;

public class DataPsiAugmentProvider extends PsiAugmentProvider {
    //TODO: I'm not sure if the following is possible, but if it is it would be cool:
    // 1. When I ctrl+click on a builder method or query where clause method, it should take me to the field definition in the data class.
    // 2. When I refactor a field in the data class, the corresponding builder method and query where clause methods should also be refactored.

    //TODO: This seems to work fine, but tests should probably be added just in case.
    //TODO: Add javadocs to generated methods and classes.
    //TODO: Make the IDE give inline warnings/errors if static-data is used wrong. I.e. define a non-abstract class that extends UniqueData without the @Data annotation.

    private static final Key<CachedValue<PsiClass>> BUILDER_CLASS_KEY = Key.create("synthetic.class.builder");
    private static final Key<CachedValue<PsiMethod>> BUILDER_METHOD_KEY = Key.create("synthetic.method.builder");
    private static final Key<CachedValue<PsiClass>> QUERY_CLASS_KEY = Key.create("synthetic.class.query");
    private static final Key<CachedValue<PsiMethod>> QUERY_METHOD_KEY = Key.create("synthetic.method.query");
    private static final Key<CachedValue<PsiClass>> QUERY_WHERE_CLASS_KEY = Key.create("synthetic.class.query.where");


    @Override
    protected @NotNull <Psi extends PsiElement> List<Psi> getAugments(@NotNull PsiElement element, @NotNull Class<Psi> type, @Nullable String nameHint) {
        if (!(element instanceof PsiClass psiClass)) {
            return Collections.emptyList();
        }

        if (!Utils.extendsClass(psiClass, Constants.UNIQUE_DATA_FQN)) {
            return Collections.emptyList();
        }

        if (!Utils.hasAnnotation(psiClass, Constants.DATA_ANNOTATION_FQN)) {
            return Collections.emptyList();
        }

        if (type.isAssignableFrom(PsiClass.class)) {
            return List.of(type.cast(getBuilderClass(psiClass)), type.cast(getQueryClass(psiClass)));
        }

        if (type.isAssignableFrom(PsiMethod.class)) {
            return List.of(type.cast(getBuilderMethod(psiClass)), type.cast(getQueryMethod(psiClass)));
        }

        return Collections.emptyList();
    }

    private PsiClass getBuilderClass(PsiClass parent) {
        return CachedValuesManager.getCachedValue(parent, BUILDER_CLASS_KEY, () -> {
            PsiClass builderClass = createBuilderBuilderClass(parent);
            return CachedValueProvider.Result.create(builderClass, parent);
        });
    }

    private PsiClass getQueryClass(PsiClass parent) {
        return CachedValuesManager.getCachedValue(parent, QUERY_CLASS_KEY, () -> {
            PsiClass queryClass = createQueryBuilderClass(parent);
            return CachedValueProvider.Result.create(queryClass, parent);
        });
    }

    private PsiClass getQueryWhereClass(PsiClass parent) {
        return CachedValuesManager.getCachedValue(parent, QUERY_WHERE_CLASS_KEY, () -> {
            PsiClass queryWhereClass = createQueryWhereBuilderClass(parent);
            return CachedValueProvider.Result.create(queryWhereClass, parent);
        });
    }

    private PsiMethod getBuilderMethod(PsiClass parent) {
//        return CachedValuesManager.getCachedValue(parent, () -> {
//            PsiClass builderClass = getBuilderClass(parent);
//            PsiType returnType = JavaPsiFacade.getElementFactory(parent.getProject())
//                    .createType(builderClass, PsiSubstitutor.EMPTY);
//            SyntheticBuilderMethod builderMethod = new SyntheticBuilderMethod(parent, "builder", returnType);
//            return CachedValueProvider.Result.create(builderMethod, parent);
//        });
        PsiClass builderClass = getBuilderClass(parent);
        PsiType returnType = JavaPsiFacade.getElementFactory(parent.getProject())
                .createType(builderClass, PsiSubstitutor.EMPTY);
        SyntheticMethod builderMethod = new SyntheticMethod(parent, parent, "builder", returnType);
        builderMethod.addModifier(PsiModifier.PUBLIC);
        builderMethod.addModifier(PsiModifier.STATIC);
        builderMethod.addModifier(PsiModifier.FINAL);
        return builderMethod;
    }

    private PsiMethod getQueryMethod(PsiClass parent) {
//        return CachedValuesManager.getCachedValue(parent, () -> {
//            PsiClass builderClass = getBuilderClass(parent);
//            PsiType returnType = JavaPsiFacade.getElementFactory(parent.getProject())
//                    .createType(builderClass, PsiSubstitutor.EMPTY);
//            SyntheticBuilderMethod builderMethod = new SyntheticBuilderMethod(parent, "builder", returnType);
//            return CachedValueProvider.Result.create(builderMethod, parent);
//        });
        PsiClass queryClass = getQueryClass(parent);
        PsiType returnType = JavaPsiFacade.getElementFactory(parent.getProject())
                .createType(queryClass, PsiSubstitutor.EMPTY);
        SyntheticMethod builderMethod = new SyntheticMethod(parent, parent, "query", returnType);
        builderMethod.addModifier(PsiModifier.PUBLIC);
        builderMethod.addModifier(PsiModifier.STATIC);
        builderMethod.addModifier(PsiModifier.FINAL);
        return builderMethod;
    }

    private SyntheticBuilderClass createBuilderBuilderClass(PsiClass parentClass) {
        SyntheticBuilderClass builderClass = new SyntheticBuilderClass(parentClass, "Builder");
        PsiType builderType = JavaPsiFacade.getElementFactory(parentClass.getProject())
                .createType(builderClass, PsiSubstitutor.EMPTY);
        for (PsiField psiField : parentClass.getAllFields()) {
            PsiType type = psiField.getType();
            PsiType innerType = Utils.getGenericParameter((PsiClassType) type, parentClass.getManager());
            if (Utils.isValidPersistentValue(psiField)) {
                SyntheticMethod setterMethod = new SyntheticMethod(parentClass, builderClass, psiField.getName(), builderType);
                setterMethod.addParameter(psiField.getName(), innerType);
                setterMethod.addModifier(PsiModifier.PUBLIC);
                setterMethod.addModifier(PsiModifier.FINAL);

                builderClass.addMethod(setterMethod);
            } else if (Utils.isValidReference(psiField)) {
                SyntheticMethod setterMethod = new SyntheticMethod(parentClass, builderClass, psiField.getName(), builderType);
                setterMethod.addParameter(psiField.getName(), innerType);
                setterMethod.addModifier(PsiModifier.PUBLIC);
                setterMethod.addModifier(PsiModifier.FINAL);

                builderClass.addMethod(setterMethod);
            }
            //todo: support CachedValues, similar to PVs
        }

        return builderClass;
    }

    private SyntheticBuilderClass createQueryBuilderClass(PsiClass parentClass) {
        SyntheticBuilderClass queryClass = new SyntheticBuilderClass(parentClass, "Query");
        PsiClass whereClass = getQueryWhereClass(parentClass);
        PsiType whereType = JavaPsiFacade.getElementFactory(parentClass.getProject())
                .createType(whereClass, PsiSubstitutor.EMPTY);
        PsiType queryType = JavaPsiFacade.getElementFactory(parentClass.getProject())
                .createType(queryClass, PsiSubstitutor.EMPTY);
        PsiType parentType = JavaPsiFacade.getElementFactory(parentClass.getProject())
                .createType(parentClass, PsiSubstitutor.EMPTY);

        PsiType intType = JavaPsiFacade.getElementFactory(parentClass.getProject())
                .createTypeFromText("int", parentClass);

        PsiType orderType = JavaPsiFacade.getElementFactory(parentClass.getProject())
                .createTypeFromText(Constants.ORDER_FQN, parentClass);


        PsiClass listClass = JavaPsiFacade.getInstance(parentClass.getProject())
                .findClass(List.class.getName(), GlobalSearchScope.allScope(parentClass.getProject()));
        assert listClass != null;

        PsiClass functionClass = JavaPsiFacade.getInstance(parentClass.getProject())
                .findClass(Function.class.getName(), GlobalSearchScope.allScope(parentClass.getProject()));
        assert functionClass != null;
        PsiSubstitutor substitutor = PsiSubstitutor.EMPTY
                .put(functionClass.getTypeParameters()[0], whereType)
                .put(functionClass.getTypeParameters()[1], whereType);
        PsiType whereFunctionType = JavaPsiFacade.getElementFactory(parentClass.getProject())
                .createType(functionClass, substitutor);

        substitutor = PsiSubstitutor.EMPTY
                .put(listClass.getTypeParameters()[0], parentType);
        PsiType listOfParentType = JavaPsiFacade.getElementFactory(parentClass.getProject())
                .createType(listClass, substitutor);

        for (PsiField psiField : parentClass.getAllFields()) {
            if (Utils.isValidPersistentValue(psiField)) {
                SyntheticMethod orderByMethod = new SyntheticMethod(parentClass, queryClass, "orderBy" + StringUtil.capitalize(psiField.getName()), queryType);
                orderByMethod.addParameter("order", orderType);
                orderByMethod.addModifier(PsiModifier.PUBLIC);
                orderByMethod.addModifier(PsiModifier.FINAL);
                queryClass.addMethod(orderByMethod);
            }
        }

        SyntheticMethod whereMethod = new SyntheticMethod(parentClass, queryClass, "where", queryType);
        whereMethod.addModifier(PsiModifier.PUBLIC);
        whereMethod.addModifier(PsiModifier.FINAL);
        whereMethod.addParameter("where", whereFunctionType);
        queryClass.addMethod(whereMethod);
        SyntheticMethod limitMethod = new SyntheticMethod(parentClass, queryClass, "limit", queryType);
        limitMethod.addParameter("limit", intType);
        limitMethod.addModifier(PsiModifier.PUBLIC);
        limitMethod.addModifier(PsiModifier.FINAL);
        queryClass.addMethod(limitMethod);
        SyntheticMethod offsetMethod = new SyntheticMethod(parentClass, queryClass, "offset", queryType);
        offsetMethod.addParameter("offset", intType);
        offsetMethod.addModifier(PsiModifier.PUBLIC);
        offsetMethod.addModifier(PsiModifier.FINAL);
        queryClass.addMethod(offsetMethod);

        SyntheticMethod findAllMethod = new SyntheticMethod(parentClass, queryClass, "findAll", listOfParentType);
        findAllMethod.addModifier(PsiModifier.PUBLIC);
        findAllMethod.addModifier(PsiModifier.FINAL);
        queryClass.addMethod(findAllMethod);
        SyntheticMethod findOneMethod = new SyntheticMethod(parentClass, queryClass, "findOne", parentType);
        findOneMethod.addModifier(PsiModifier.PUBLIC);
        findOneMethod.addModifier(PsiModifier.FINAL);
        queryClass.addMethod(findOneMethod);

        return queryClass;
    }

    private SyntheticBuilderClass createQueryWhereBuilderClass(PsiClass parentClass) {
        SyntheticBuilderClass whereClass = new SyntheticBuilderClass(parentClass, "QueryWhere");
        PsiType whereType = JavaPsiFacade.getElementFactory(parentClass.getProject())
                .createType(whereClass, PsiSubstitutor.EMPTY);

        PsiClass functionClass = JavaPsiFacade.getInstance(parentClass.getProject())
                .findClass(Function.class.getName(), GlobalSearchScope.allScope(parentClass.getProject()));
        assert functionClass != null;
        PsiSubstitutor substitutor = PsiSubstitutor.EMPTY
                .put(functionClass.getTypeParameters()[0], whereType)
                .put(functionClass.getTypeParameters()[1], whereType);
        PsiType functionParenType = JavaPsiFacade.getElementFactory(parentClass.getProject())
                .createType(functionClass, substitutor);


        SyntheticMethod andMethod = new SyntheticMethod(parentClass, whereClass, "and", whereType);
        andMethod.addModifier(PsiModifier.PUBLIC);
        andMethod.addModifier(PsiModifier.FINAL);
        whereClass.addMethod(andMethod);
        SyntheticMethod orMethod = new SyntheticMethod(parentClass, whereClass, "or", whereType);
        orMethod.addModifier(PsiModifier.PUBLIC);
        orMethod.addModifier(PsiModifier.FINAL);
        whereClass.addMethod(orMethod);

        // ( [clause] )
        SyntheticMethod groupMethod = new SyntheticMethod(parentClass, whereClass, "group", whereType);
        groupMethod.addParameter("clause", functionParenType);
        groupMethod.addModifier(PsiModifier.PUBLIC);
        groupMethod.addModifier(PsiModifier.FINAL);
        whereClass.addMethod(groupMethod);
        for (PsiField psiField : parentClass.getAllFields()) {
            PsiType type = psiField.getType();
            boolean isValidReference = false;
            if (!Utils.isValidPersistentValue(psiField) && !(isValidReference = Utils.isValidReference(psiField))) {
                continue; //non-supported field type
            }
            PsiType innerType = Utils.getGenericParameter((PsiClassType) type, parentClass.getManager());

            List<QueryClause> clauses = QueryBuilderUtils.getClausesForType(psiField, isValidReference || Utils.isNullable(psiField, type));
            for (QueryClause clause : clauses) {
                String methodName = clause.getMethodName(psiField.getName());
                List<PsiType> parameterTypes = clause.getMethodParamTypes(parentClass.getManager(), innerType);
                SyntheticMethod queryMethod = new SyntheticMethod(parentClass, whereClass, methodName, whereType);
                for (int i = 0; i < parameterTypes.size(); i++) {
                    queryMethod.addParameter(psiField.getName() + i, parameterTypes.get(i));
                }
                queryMethod.addModifier(PsiModifier.PUBLIC);
                queryMethod.addModifier(PsiModifier.FINAL);
                whereClass.addMethod(queryMethod);
            }
        }

        return whereClass;
    }
}