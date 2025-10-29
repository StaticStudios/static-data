package net.staticstudios.data.compiler.javac;

public abstract class ParsedAnnotation {
    private final String annotationFQN;

    public ParsedAnnotation(String annotationFQN) {
        this.annotationFQN = annotationFQN;
    }

    public String getAnnotationFQN() {
        return annotationFQN;
    }

}
