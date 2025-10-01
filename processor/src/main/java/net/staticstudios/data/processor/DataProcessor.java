package net.staticstudios.data.processor;

import com.palantir.javapoet.*;
import net.staticstudios.data.Data;
import net.staticstudios.data.InsertStrategy;

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
public class DataProcessor extends AbstractProcessor { //todo: this seems to be in the classpath of the main project. address this.
    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        for (Element annotated : roundEnv.getElementsAnnotatedWith(Data.class)) {
            if (!(annotated instanceof TypeElement type)) continue;

            try {
                Data dataAnnotation = type.getAnnotation(Data.class);
                assert dataAnnotation != null;
                List<Metadata> metadataList = MetadataUtils.extractMetadata(type);

                generateFactory(type, dataAnnotation, metadataList);
                new QueryFactory(processingEnv, type, dataAnnotation, metadataList).generateQueryBuilder();
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

    private void generateFactory(TypeElement entityType, Data dataAnnotation, List<Metadata> metadataList) throws IOException {
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


        TypeSpec.Builder factoryBuilder = TypeSpec.classBuilder(factoryName);
        TypeSpec.Builder builderType = TypeSpec.classBuilder("Builder")
                .addModifiers(Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
                .addField(dataManager, "dataManager", Modifier.PRIVATE, Modifier.FINAL)
                .addMethod(MethodSpec.constructorBuilder()
                        .addParameter(dataManager, "dataManager")
                        .addStatement("this.dataManager = dataManager")
                        .build());

        //todo: support collections and references.


        MethodSpec.Builder insertCtxMethod = MethodSpec.methodBuilder("insert")
                .addModifiers(Modifier.PUBLIC)
                .returns(TypeName.VOID)
                .addParameter(insertContext, "ctx");

        for (Metadata metadata : metadataList) {
            if (metadata instanceof PersistentValueMetadata persistentValueMetadata) {
                SchemaTableColumnStatics statics = SchemaTableColumnStatics.generateSchemaTableColumnStatics(builderType, persistentValueMetadata);
                builderType.addField(persistentValueMetadata.genericType(), persistentValueMetadata.fieldName(), Modifier.PRIVATE);

                builderType.addMethod(MethodSpec.methodBuilder(persistentValueMetadata.fieldName())
                        .addModifiers(Modifier.PUBLIC)
                        .returns(ClassName.get(packageName, factoryName, "Builder"))
                        .addParameter(persistentValueMetadata.genericType(), persistentValueMetadata.fieldName())
                        .addStatement("this.$N = $N", persistentValueMetadata.fieldName(), persistentValueMetadata.fieldName())
                        .addStatement("return this")
                        .build());

                if (persistentValueMetadata instanceof ForeignPersistentValueMetadata foreignPersistentValueMetadata) {
                    insertCtxMethod.beginControlFlow("if (this.$N != null)", persistentValueMetadata.fieldName());
                    insertCtxMethod.addStatement("ctx.set($N, $N, $N, this.$N, $T.$L)",
                            statics.schemaFieldName(),
                            statics.tableFieldName(),
                            statics.columnFieldName(),
                            persistentValueMetadata.fieldName(),
                            InsertStrategy.class,
                            foreignPersistentValueMetadata.insertStrategy());
                } else {
                    insertCtxMethod.addStatement("ctx.set($N, $N, $N, this.$N, null)",
                            statics.schemaFieldName(),
                            statics.tableFieldName(),
                            statics.columnFieldName(),
                            persistentValueMetadata.fieldName());
                }

                if (persistentValueMetadata instanceof ForeignPersistentValueMetadata foreignPersistentValueMetadata) {
                    for (ForeignLink link : MetadataUtils.makeFPVStatics(builderType, foreignPersistentValueMetadata, metadataList, dataAnnotation, statics)) {
                        insertCtxMethod.addStatement("ctx.set($N, $N, $N, this.$N, null)",
                                statics.schemaFieldName(),
                                statics.tableFieldName(),
                                link.foreignColumnFieldName(),
                                link.localColumnMetadata().fieldName());
                    }
                    insertCtxMethod.endControlFlow();
                }
            }
        }

        MethodSpec.Builder insertModeMethod = MethodSpec.methodBuilder("insert")
                .addModifiers(Modifier.PUBLIC)
                .returns(entityClass)
                .addParameter(insertMode, "mode")
                .addStatement("$T ctx = dataManager.createInsertContext()", insertContext)
                .addStatement("this.insert(ctx)")
                .addStatement("return ctx.insert(mode).get($T.class)", entityClass);

        builderType.addMethod(insertModeMethod.build());
        builderType.addMethod(insertCtxMethod.build());

        TypeSpec factory = factoryBuilder
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