package net.staticstudios.data.ide.intellij;

import com.intellij.psi.*;

import java.util.Objects;

public class Utils {
    public static boolean is(PsiType type, String classFqn) {
        if (!(type instanceof PsiClassType psiClassType)) {
            return false;
        }
        PsiClass resolvedClass = psiClassType.resolve();
        if (resolvedClass == null) {
            return false;
        }
        return classFqn.equals(resolvedClass.getQualifiedName());
    }

    public static boolean extendsClass(PsiClass psiClass, String classFqn) {
        boolean extendsClass = false;
        for (PsiClassType superType : psiClass.getSuperTypes()) {
            String superTypeFqn = superType.resolve() != null ? Objects.requireNonNull(superType.resolve()).getQualifiedName() : null;
            if (classFqn.equals(superTypeFqn)) {
                extendsClass = true;
                break;
            }
        }

        return extendsClass;
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
        return Utils.is(psiField.getType(), Constants.PERSISTENT_VALUE_FQN) && (
                Utils.hasAnnotation(psiField, Constants.COLUMN_ANNOTATION_FQN) ||
                        Utils.hasAnnotation(psiField, Constants.FOREIGN_COLUMN_ANNOTATION_FQN) ||
                        Utils.hasAnnotation(psiField, Constants.ID_COLUMN_ANNOTATION_FQN));
    }

    public static boolean isValidReference(PsiField psiField) {
        return Utils.is(psiField.getType(), Constants.REFERENCE_FQN) &&
                Utils.hasAnnotation(psiField, Constants.ONE_TO_ONE_ANNOTATION_FQN);
    }
}
