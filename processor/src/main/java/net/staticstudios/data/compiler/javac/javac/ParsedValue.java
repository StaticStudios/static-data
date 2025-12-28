package net.staticstudios.data.compiler.javac.javac;

import javax.lang.model.element.TypeElement;

public class ParsedValue {
    protected final String fieldName;
    protected final TypeElement type;

    public ParsedValue(String fieldName, TypeElement type) {
        this.fieldName = fieldName;
        this.type = type;
    }

    public String getFieldName() {
        return fieldName;
    }

    public TypeElement getType() {
        return type;
    }
}
