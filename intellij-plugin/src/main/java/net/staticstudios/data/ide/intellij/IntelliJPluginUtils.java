package net.staticstudios.data.ide.intellij;

import com.intellij.psi.*;
import net.staticstudios.data.utils.Constants;

public class IntelliJPluginUtils {
    public static boolean genericTypeIs(PsiType type, String classFqn) {
        if (!(type instanceof PsiClassType psiClassType)) {
            return false;
        }
        PsiType[] params = psiClassType.getParameters();
        if (params.length == 0) {
            return false;
        }

        return is(params[0], classFqn);
    }

    public static boolean is(PsiType type, String classFqn) {
        if (!(type instanceof PsiClassType psiClassType)) {
            return false;
        }
        String canonicalText = psiClassType.rawType().getCanonicalText();
        return classFqn.equals(canonicalText);
    }

    public static boolean extendsClass(PsiClass psiClass, String classFqn) {
        for (PsiClass superClass : psiClass.getSupers()) {
            String superFqn = superClass.getQualifiedName();
            if (classFqn.equals(superFqn)) {
                return true;
            }
            if (superFqn != null
                    && !superFqn.equals(Object.class.getName())
                    && extendsClass(superClass, classFqn)) {
                return true;
            }
        }
        return false;
    }

    public static boolean hasAnnotation(PsiModifierListOwner element, String annotationFqn) {
        if (element.getModifierList() == null) {
            return false;
        }
        return element.getModifierList().findAnnotation(annotationFqn) != null;
    }

    public static PsiType getGenericParameter(PsiClassType type, PsiManager manager) {
        PsiType[] params = type.getParameters();
        return (params.length > 0) ? params[0] : PsiType.getJavaLangObject(manager, type.getResolveScope());
    }

    public static boolean isNullable(PsiField psiField, PsiType fieldType) {
        PsiModifierList modifierList = psiField.getModifierList();
        if (modifierList == null) return false;
        if (is(fieldType, Constants.PERSISTENT_VALUE_FQN)) {
            PsiAnnotation annotation;
            annotation = modifierList.findAnnotation(Constants.COLUMN_ANNOTATION_FQN);
            if (annotation == null) {
                annotation = modifierList.findAnnotation(Constants.ID_COLUMN_ANNOTATION_FQN);
            }
            if (annotation == null) {
                annotation = modifierList.findAnnotation(Constants.FOREIGN_COLUMN_ANNOTATION_FQN);
            }
            if (annotation == null) return false;
            PsiAnnotationMemberValue memberValue = annotation.findAttributeValue("nullable");
            if (memberValue instanceof PsiLiteralExpression) {
                Object value = ((PsiLiteralExpression) memberValue).getValue();
                if (value instanceof Boolean) {
                    return (Boolean) value;
                }
            }
            return false;
        }

        return true; // assume null for other fields like Reference<T> etc...
    }

    public static boolean isValidPersistentValue(PsiField psiField) {
        return IntelliJPluginUtils.is(psiField.getType(), Constants.PERSISTENT_VALUE_FQN) && (
                IntelliJPluginUtils.hasAnnotation(psiField, Constants.COLUMN_ANNOTATION_FQN) ||
                        IntelliJPluginUtils.hasAnnotation(psiField, Constants.FOREIGN_COLUMN_ANNOTATION_FQN) ||
                        IntelliJPluginUtils.hasAnnotation(psiField, Constants.ID_COLUMN_ANNOTATION_FQN));
    }

    public static boolean isValidReference(PsiField psiField) {
        return IntelliJPluginUtils.is(psiField.getType(), Constants.REFERENCE_FQN) &&
                IntelliJPluginUtils.hasAnnotation(psiField, Constants.ONE_TO_ONE_ANNOTATION_FQN);
    }

    public static boolean isValidCachedValue(PsiField psiField) {
        return IntelliJPluginUtils.is(psiField.getType(), Constants.CACHED_VALUE_FQN) &&
                IntelliJPluginUtils.hasAnnotation(psiField, Constants.IDENTIFIER_ANNOTATION_FQN);
    }
}
