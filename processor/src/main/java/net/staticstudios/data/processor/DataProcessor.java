package net.staticstudios.data.processor;

import com.palantir.javapoet.*;
import net.staticstudios.data.Data;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.PackageElement;
import javax.lang.model.element.TypeElement;
import javax.tools.Diagnostic;
import java.io.IOException;
import java.util.List;
import java.util.Set;

@SupportedAnnotationTypes("net.staticstudios.data.Data")
@SupportedSourceVersion(SourceVersion.RELEASE_21)
public class DataProcessor extends AbstractProcessor {
    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        for (Element annotated : roundEnv.getElementsAnnotatedWith(Data.class)) {
            if (!(annotated instanceof TypeElement type)) continue;

            try {
                generateFactory(type);
            } catch (IOException e) {
                processingEnv.getMessager().printMessage(
                        Diagnostic.Kind.ERROR,
                        "Failed to generate factory: " + e.getMessage(),
                        annotated
                );
            }
        }
        return true;
    }

    private void generateFactory(TypeElement entityType) throws IOException {
        if (entityType.getModifiers().contains(Modifier.ABSTRACT)) {
            return;
        }

        String entityName = entityType.getSimpleName().toString();
        String factoryName = entityName + "Factory";
        PackageElement packageElement = processingEnv.getElementUtils().getPackageOf(entityType);
        String packageName = packageElement.isUnnamed() ? "" : packageElement.getQualifiedName().toString();

        ClassName entityClass = ClassName.get(packageName, entityName);
        ClassName dataManager = ClassName.get("net.staticstudios.data", "DataManager");
        ClassName insertMode = ClassName.get("net.staticstudios.data", "InsertMode");
        ClassName insertContext = ClassName.get("net.staticstudios.data.insert", "InsertContext");


        List<Metadata> metadataList = MetadataUtils.extractMetadata(entityType);

        TypeSpec.Builder builderType = TypeSpec.classBuilder("Builder")
                .addModifiers(Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
                .addField(dataManager, "dataManager", Modifier.PRIVATE, Modifier.FINAL)
                .addMethod(MethodSpec.constructorBuilder()
                        .addParameter(dataManager, "dataManager")
                        .addStatement("this.dataManager = dataManager")
                        .build());

        //todo: support collections and references.

        MethodSpec.Builder insertModeMethod = MethodSpec.methodBuilder("insert")
                .addModifiers(Modifier.PUBLIC)
                .returns(entityClass)
                .addParameter(insertMode, "mode")
                .addStatement("$T ctx = dataManager.createInsertContext()", insertContext);
        MethodSpec.Builder insertCtxMethod = MethodSpec.methodBuilder("insert")
                .addModifiers(Modifier.PUBLIC)
                .returns(TypeName.VOID)
                .addParameter(insertContext, "ctx");

        for (Metadata metadata : metadataList) {
            if (metadata instanceof PersistentValueMetadata(
                    String schema, String table, String column, String fieldName, TypeName genericType
            )) {
                String schemaFieldName = fieldName + "$schema";
                String tableFieldName = fieldName + "$table";
                String columnFieldName = fieldName + "$column";


                builderType.addField(genericType, fieldName, Modifier.PRIVATE);

                // since we support env variables in the name, parse these at runtime.
                builderType.addField(FieldSpec.builder(String.class, schemaFieldName, Modifier.PRIVATE, Modifier.FINAL, Modifier.STATIC)
                        .initializer("$T.parseValue($S)", ClassName.get("net.staticstudios.data.util", "ValueUtils"), schema)
                        .build());
                builderType.addField(FieldSpec.builder(String.class, tableFieldName, Modifier.PRIVATE, Modifier.FINAL, Modifier.STATIC)
                        .initializer("$T.parseValue($S)", ClassName.get("net.staticstudios.data.util", "ValueUtils"), table)
                        .build());
                builderType.addField(FieldSpec.builder(String.class, columnFieldName, Modifier.PRIVATE, Modifier.FINAL, Modifier.STATIC)
                        .initializer("$T.parseValue($S)", ClassName.get("net.staticstudios.data.util", "ValueUtils"), column)
                        .build());

                builderType.addMethod(MethodSpec.methodBuilder(fieldName)
                        .addModifiers(Modifier.PUBLIC)
                        .returns(ClassName.get(packageName, factoryName, "Builder"))
                        .addParameter(genericType, fieldName)
                        .addStatement("this.$N = $N", fieldName, fieldName)
                        .addStatement("return this")
                        .build());

                insertModeMethod.addStatement("ctx.set($N, $N, $N, this.$N)",
                        schemaFieldName,
                        tableFieldName,
                        columnFieldName,
                        fieldName);
                insertCtxMethod.addStatement("ctx.set($N, $N, $N, this.$N)",
                        schemaFieldName,
                        tableFieldName,
                        columnFieldName,
                        fieldName);
            }
        }

        insertModeMethod.addStatement("return ctx.insert(mode).get($T.class)", entityClass);
        builderType.addMethod(insertModeMethod.build());
        builderType.addMethod(insertCtxMethod.build());

        TypeSpec factory = TypeSpec.classBuilder(factoryName)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addMethod(MethodSpec.constructorBuilder()
                        .addModifiers(Modifier.PRIVATE)
                        .build())
                .addMethod(MethodSpec.methodBuilder("builder")
                        .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
                        .returns(ClassName.get(packageName, factoryName, "Builder"))
                        .addParameter(dataManager, "dataManager")
                        .addStatement("return new Builder(dataManager)")
                        .build())
                .addMethod(MethodSpec.methodBuilder("builder")
                        .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
                        .returns(ClassName.get(packageName, factoryName, "Builder"))
                        .addStatement("return new Builder(DataManager.getInstance())")
                        .build())
                .addType(builderType.build())
                .build();

        JavaFile.builder(packageName, factory)
                .indent("    ")
                .build()
                .writeTo(processingEnv.getFiler());
    }
}