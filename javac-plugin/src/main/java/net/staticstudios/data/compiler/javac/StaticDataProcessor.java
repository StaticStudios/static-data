package net.staticstudios.data.compiler.javac;

import com.sun.source.tree.Tree;
import com.sun.source.util.Trees;
import com.sun.tools.javac.processing.JavacFiler;
import com.sun.tools.javac.processing.JavacProcessingEnvironment;
import com.sun.tools.javac.tree.JCTree;
import com.sun.tools.javac.tree.TreeMaker;
import com.sun.tools.javac.util.Names;
import net.staticstudios.data.Data;
import net.staticstudios.data.compiler.javac.javac.BuilderProcessor;
import net.staticstudios.data.compiler.javac.javac.ParsedPersistentValue;
import net.staticstudios.data.compiler.javac.javac.ParsedReference;
import net.staticstudios.data.compiler.javac.javac.QueryBuilderProcessor;
import net.staticstudios.data.compiler.javac.util.TypeUtils;
import sun.misc.Unsafe;

import javax.annotation.processing.*;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.TypeElement;
import javax.lang.model.util.Elements;
import javax.tools.Diagnostic;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Collection;
import java.util.Set;

@SupportedAnnotationTypes("net.staticstudios.data.Data")
@SupportedSourceVersion(SourceVersion.RELEASE_21)
public class StaticDataProcessor extends AbstractProcessor {

    private ProcessingEnvironment processingEnvironment;
    private JavacProcessingEnvironment javacProcessingEnvironment;
    private JavacFiler javacFiler;
    private Trees trees;
    private Elements elements;
    private Names names;
    private TreeMaker treeMaker;

    private static Object getOwnModule() {
        try {
            Method m = Permit.getMethod(Class.class, "getModule");
            return m.invoke(StaticDataProcessor.class);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Useful from jdk9 and up; required from jdk16 and up. This code is supposed to gracefully do nothing on jdk8 and below, as this operation isn't needed there.
     */
    public static void addOpensForLombok() {
        Class<?> cModule;
        try {
            cModule = Class.forName("java.lang.Module");
        } catch (ClassNotFoundException e) {
            return; //jdk8-; this is not needed.
        }

        Unsafe unsafe = getUnsafe();
        Object jdkCompilerModule = getJdkCompilerModule();
        Object ownModule = getOwnModule();
        String[] allPkgs = {
                "com.sun.tools.javac.code",
                "com.sun.tools.javac.comp",
                "com.sun.tools.javac.file",
                "com.sun.tools.javac.main",
                "com.sun.tools.javac.model",
                "com.sun.tools.javac.parser",
                "com.sun.tools.javac.processing",
                "com.sun.tools.javac.tree",
                "com.sun.tools.javac.util",
                "com.sun.tools.javac.jvm",
        };

        try {
            Method m = cModule.getDeclaredMethod("implAddOpens", String.class, cModule);
            long firstFieldOffset = getFirstFieldOffset(unsafe);
            unsafe.putBooleanVolatile(m, firstFieldOffset, true);
            for (String p : allPkgs) m.invoke(jdkCompilerModule, p, ownModule);
        } catch (Exception ignore) {
        }
    }

    private static Object getJdkCompilerModule() {
		/* call public api: ModuleLayer.boot().findModule("jdk.compiler").get();
		   but use reflection because we don't want this code to crash on jdk1.7 and below.
		   In that case, none of this stuff was needed in the first place, so we just exit via
		   the catch block and do nothing.
		 */

        try {
            Class<?> cModuleLayer = Class.forName("java.lang.ModuleLayer");
            Method mBoot = cModuleLayer.getDeclaredMethod("boot");
            Object bootLayer = mBoot.invoke(null);
            Class<?> cOptional = Class.forName("java.util.Optional");
            Method mFindModule = cModuleLayer.getDeclaredMethod("findModule", String.class);
            Object oCompilerO = mFindModule.invoke(bootLayer, "jdk.compiler");
            return cOptional.getDeclaredMethod("get").invoke(oCompilerO);
        } catch (Exception e) {
            return null;
        }
    }

    private static long getFirstFieldOffset(Unsafe unsafe) {
        try {
            return unsafe.objectFieldOffset(Parent.class.getDeclaredField("first"));
        } catch (NoSuchFieldException e) {
            // can't happen.
            throw new RuntimeException(e);
        } catch (SecurityException e) {
            // can't happen
            throw new RuntimeException(e);
        }
    }

    private static Unsafe getUnsafe() {
        try {
            Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            return (Unsafe) theUnsafe.get(null);
        } catch (Exception e) {
            return null;
        }
    }


    @Override
    public synchronized void init(ProcessingEnvironment processingEnv) {
        super.init(processingEnv);
        this.processingEnvironment = processingEnv;
        this.javacProcessingEnvironment = getJavacProcessingEnvironment(processingEnv);
        this.javacFiler = getJavacFiler(processingEnv.getFiler());
        this.trees = Trees.instance(processingEnv);
        this.elements = processingEnv.getElementUtils();
        this.names = Names.instance(javacProcessingEnvironment.getContext());
        this.treeMaker = TreeMaker.instance(javacProcessingEnvironment.getContext());
    }

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        Set<? extends Element> annotated = roundEnv.getElementsAnnotatedWith(Data.class);
        annotated.forEach(e -> {
            TypeElement typeElement = (TypeElement) e;
            Tree tree = trees.getTree(e);
            TypeUtils typeUtils = new TypeUtils(processingEnvironment);
            JCTree.JCClassDecl classDecl = (JCTree.JCClassDecl) tree;
            if (!BuilderProcessor.hasProcessed(classDecl)) {
                Data dataAnnotation = e.getAnnotation(Data.class);
                Collection<ParsedPersistentValue> persistentValues = ParsedPersistentValue.extractPersistentValues(typeElement, dataAnnotation, typeUtils);
                Collection<ParsedReference> references = ParsedReference.extractReferences(typeElement, dataAnnotation, typeUtils);
                ProcessorContext processorContext = new ProcessorContext(
                        javacProcessingEnvironment.getContext(),
                        trees,
                        new TypeUtils(processingEnvironment),
                        dataAnnotation,
                        (TypeElement) e,
                        classDecl,
                        persistentValues,
                        references
                );
                new BuilderProcessor(processorContext).runProcessor();
                new QueryBuilderProcessor(processorContext).runProcessor();
            }
        });
        return !annotations.isEmpty();
    }

    /**
     * This class casts the given processing environment to a JavacProcessingEnvironment. In case of
     * gradle incremental compilation, the delegate ProcessingEnvironment of the gradle wrapper is returned.
     */
    public JavacProcessingEnvironment getJavacProcessingEnvironment(Object procEnv) {
        addOpensForLombok();
        if (procEnv instanceof JavacProcessingEnvironment) return (JavacProcessingEnvironment) procEnv;

        // try to find a "delegate" field in the object, and use this to try to obtain a JavacProcessingEnvironment
        for (Class<?> procEnvClass = procEnv.getClass(); procEnvClass != null; procEnvClass = procEnvClass.getSuperclass()) {
            Object delegate = tryGetDelegateField(procEnvClass, procEnv);
            if (delegate == null) delegate = tryGetProxyDelegateToField(procEnvClass, procEnv);
            if (delegate == null) delegate = tryGetProcessingEnvField(procEnvClass, procEnv);

            if (delegate != null) return getJavacProcessingEnvironment(delegate);
            // delegate field was not found, try on superclass
        }

        processingEnv.getMessager().printMessage(Diagnostic.Kind.WARNING,
                "Can't get the delegate of the gradle IncrementalProcessingEnvironment. Lombok won't work.");
        return null;
    }

    /**
     * This class returns the given filer as a JavacFiler. In case the filer is no
     * JavacFiler (e.g. the Gradle IncrementalFiler), its "delegate" field is used to get the JavacFiler
     * (directly or through a delegate field again)
     */
    public JavacFiler getJavacFiler(Object filer) {
        if (filer instanceof JavacFiler) return (JavacFiler) filer;

        // try to find a "delegate" field in the object, and use this to check for a JavacFiler
        for (Class<?> filerClass = filer.getClass(); filerClass != null; filerClass = filerClass.getSuperclass()) {
            Object delegate = tryGetDelegateField(filerClass, filer);
            if (delegate == null) delegate = tryGetProxyDelegateToField(filerClass, filer);
            if (delegate == null) delegate = tryGetFilerField(filerClass, filer);

            if (delegate != null) return getJavacFiler(delegate);
            // delegate field was not found, try on superclass
        }

        processingEnv.getMessager().printMessage(Diagnostic.Kind.WARNING,
                "Can't get a JavacFiler from " + filer.getClass().getName() + ". Lombok won't work.");
        return null;
    }

    /**
     * Gradle incremental processing
     */
    private Object tryGetDelegateField(Class<?> delegateClass, Object instance) {
        try {
            return Permit.getField(delegateClass, "delegate").get(instance);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Kotlin incremental processing
     */
    private Object tryGetProcessingEnvField(Class<?> delegateClass, Object instance) {
        try {
            return Permit.getField(delegateClass, "processingEnv").get(instance);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Kotlin incremental processing
     */
    private Object tryGetFilerField(Class<?> delegateClass, Object instance) {
        try {
            return Permit.getField(delegateClass, "filer").get(instance);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * IntelliJ IDEA >= 2020.3
     */
    private Object tryGetProxyDelegateToField(Class<?> delegateClass, Object instance) {
        try {
            InvocationHandler handler = Proxy.getInvocationHandler(instance);
            return Permit.getField(handler.getClass(), "val$delegateTo").get(handler);
        } catch (Exception e) {
            return null;
        }
    }
}