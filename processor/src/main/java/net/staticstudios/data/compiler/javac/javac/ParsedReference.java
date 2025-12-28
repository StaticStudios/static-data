package net.staticstudios.data.compiler.javac.javac;

import net.staticstudios.data.Data;
import net.staticstudios.data.OneToOne;
import net.staticstudios.data.compiler.javac.util.SimpleField;
import net.staticstudios.data.compiler.javac.util.TypeUtils;
import net.staticstudios.data.utils.Constants;
import net.staticstudios.data.utils.Link;
import org.jetbrains.annotations.NotNull;

import javax.lang.model.element.Element;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeMirror;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class ParsedReference extends ParsedValue implements ParsedRelation {
    private final List<Link> links;

    public ParsedReference(String fieldName, List<Link> links, TypeElement type) {
        super(fieldName, type);
        this.links = links;
    }

    public static Collection<ParsedReference> extractReferences(@NotNull TypeElement dataClass,
                                                                @NotNull Data dataAnnotation,
                                                                @NotNull TypeUtils typeUtils

    ) {
        Collection<ParsedReference> references = new ArrayList<>();
        Collection<SimpleField> fields = typeUtils.getFields(dataClass, Constants.REFERENCE_FQN);
        for (SimpleField refField : fields) {
            Element fieldElement = refField.element();
            OneToOne oneToOneAnnotation = fieldElement.getAnnotation(OneToOne.class);
            if (oneToOneAnnotation == null) {
                continue;
            }

            TypeMirror genericTypeMirror = typeUtils.getGenericType(fieldElement, 0);
            TypeElement typeElement = (TypeElement) ((DeclaredType) genericTypeMirror).asElement();
            ParsedReference parsedReference = new ParsedReference(
                    refField.name(),
                    Link.parseRawLinks(oneToOneAnnotation.link()),
                    typeElement
            );
            references.add(parsedReference);
        }

        return references;
    }

    public List<Link> getLinks() {
        return links;
    }

    public String[] getTypeFQNParts() {
        return type.getQualifiedName().toString().split("\\.");
    }

    @Override
    public String toString() {
        return "ParsedReference{" +
                "fieldName='" + fieldName + '\'' +
                ", links=" + links +
                ", type=" + type +
                '}';
    }
}
