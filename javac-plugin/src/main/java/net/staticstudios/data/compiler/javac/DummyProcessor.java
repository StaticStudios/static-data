package net.staticstudios.data.compiler.javac;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.TypeElement;
import java.util.Set;

/**
 * This processor does nothing. It exists so that gradle properly picks up the javac plugin.
 */
@SupportedAnnotationTypes("net.staticstudios.data.Data")
@SupportedSourceVersion(SourceVersion.RELEASE_21)
public class DummyProcessor extends AbstractProcessor {
    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        return true;
    }
}