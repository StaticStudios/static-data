package net.staticstudios.data.compiler.javac.util;

import com.google.common.base.Preconditions;
import org.jetbrains.annotations.NotNull;

import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class TypeUtils {
    private final Elements elements;
    private final Types types;

    public TypeUtils(ProcessingEnvironment processingEnv) {
        this.elements = processingEnv.getElementUtils();
        this.types = processingEnv.getTypeUtils();
    }

    public boolean isNumericType(TypeElement typeElement) {
        if (typeElement == null) {
            return false;
        }

        String fqn = typeElement.getQualifiedName().toString();
        return fqn.equals("java.lang.Byte") ||
                fqn.equals("java.lang.Short") ||
                fqn.equals("java.lang.Integer") ||
                fqn.equals("java.lang.Long") ||
                fqn.equals("java.lang.Float") ||
                fqn.equals("java.lang.Double") ||
                fqn.equals("java.math.BigInteger") ||
                fqn.equals("java.math.BigDecimal");
    }

    public boolean isType(TypeElement typeElement, Class<?> clazz) {
        if (typeElement == null || clazz == null) {
            return false;
        }

        String fqn = typeElement.getQualifiedName().toString();
        return fqn.equals(clazz.getCanonicalName());
    }

    public Collection<SimpleField> getFields(TypeElement typeElement, String targetFQN) {
        Collection<SimpleField> fields = new ArrayList<>();
        discoverFields(typeElement, targetFQN, fields);
        return fields;
    }

    private void discoverFields(Element element, @NotNull String targetFQN, Collection<SimpleField> fields) {
        if (element == null) {
            return;
        }

        TypeElement targetTypeElement;
        TypeMirror targetTypeMirror = null;
        targetTypeElement = elements.getTypeElement(targetFQN);
        if (targetTypeElement != null) {
            targetTypeMirror = targetTypeElement.asType();
        }
        Preconditions.checkNotNull(targetTypeMirror);

        for (Element enclosed : element.getEnclosedElements()) {
            if (enclosed.getKind() == ElementKind.FIELD) {
                TypeMirror fieldType = null;
                try {
                    fieldType = enclosed.asType();
                } catch (UnsupportedOperationException ignored) {
                }

                boolean matches = false;

                if (fieldType != null) {
                    try {
                        TypeMirror fieldErasure = types.erasure(fieldType);
                        TypeMirror targetErasure = types.erasure(targetTypeMirror);

                        if (types.isSameType(fieldErasure, targetErasure)) {
                            matches = true;
                        }
                    } catch (Exception ignored) {
                    }
                }

                if (matches) {
                    fields.add(new SimpleField(enclosed.getSimpleName().toString(), enclosed));
                }
            }
        }

        if (element instanceof TypeElement typeEl) {
            TypeMirror superType = typeEl.getSuperclass();
            if (superType != null && superType.getKind() == TypeKind.DECLARED) {
                Element superElem = types.asElement(superType);
                discoverFields(superElem, targetFQN, fields);
            }
        }
    }


    public TypeMirror getGenericType(Element element, int index) {
        if (element == null || index < 0) {
            return null;
        }

        TypeMirror mirror;
        try {
            mirror = element.asType();
        } catch (UnsupportedOperationException e) {
            return null;
        }

        if (mirror == null) {
            return null;
        }

        if (mirror.getKind() == TypeKind.DECLARED && mirror instanceof DeclaredType declared) {
            List<? extends TypeMirror> args = declared.getTypeArguments();
            if (args == null || index >= args.size()) {
                return null;
            }
            return args.get(index);
        }

        return null;
    }
}
