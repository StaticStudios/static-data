package net.staticstudios.data.processor;

import com.palantir.javapoet.*;
import net.staticstudios.data.Column;
import net.staticstudios.data.Data;
import net.staticstudios.data.ForeignColumn;
import net.staticstudios.data.IdColumn;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.*;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.ElementFilter;
import javax.tools.Diagnostic;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@SupportedAnnotationTypes("net.staticstudios.data.annotations.Data")
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

    private void generateFactory(TypeElement entityType) throws IOException { //todo: if the class is abstract dont generate it. furthermore, we need to handle inheritance properly.
        String entityName = entityType.getSimpleName().toString();
        String factoryName = entityName + "Factory";
        PackageElement pkg = processingEnv.getElementUtils().getPackageOf(entityType);
        String packageName = pkg.isUnnamed() ? "" : pkg.getQualifiedName().toString();

        ClassName entityClass = ClassName.get(packageName, entityName);
        ClassName dataManager = ClassName.get("net.staticstudios.data", "DataManager");
        ClassName insertMode = ClassName.get("net.staticstudios.data.annotations", "InsertMode");
        ClassName insertContext = ClassName.get("net.staticstudios.data.insert", "InsertContext");


        List<Metadata> valueMetaData = collectProperties(entityType, processingEnv.getElementUtils().getTypeElement("net.staticstudios.data.util.Value"), entityType.getAnnotation(Data.class));

        TypeSpec.Builder builderType = TypeSpec.classBuilder("Builder")
                .addModifiers(Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL);

        builderType.addField(dataManager, "dataManager", Modifier.PRIVATE, Modifier.FINAL);

        builderType.addMethod(MethodSpec.constructorBuilder()
                .addParameter(dataManager, "dataManager")
                .addStatement("this.dataManager = dataManager")
                .build());

        //todo: support collections and references.

        for (Metadata metadata : valueMetaData) {
            builderType.addField(metadata.typeName(), metadata.fieldName(), Modifier.PRIVATE);

            // since we support env variables in the name, parse these at runtime.
            builderType.addField(FieldSpec.builder(String.class, metadata.fieldName() + "$Schema", Modifier.PRIVATE, Modifier.FINAL, Modifier.STATIC)
                    .initializer("$T.parseValue($S)", ClassName.get("net.staticstudios.data.util", "ValueUtils"), metadata.schema())
                    .build());
            builderType.addField(FieldSpec.builder(String.class, metadata.fieldName() + "$Table", Modifier.PRIVATE, Modifier.FINAL, Modifier.STATIC)
                    .initializer("$T.parseValue($S)", ClassName.get("net.staticstudios.data.util", "ValueUtils"), metadata.table())
                    .build());
            builderType.addField(FieldSpec.builder(String.class, metadata.fieldName() + "$Column", Modifier.PRIVATE, Modifier.FINAL, Modifier.STATIC)
                    .initializer("$T.parseValue($S)", ClassName.get("net.staticstudios.data.util", "ValueUtils"), metadata.column())
                    .build());

            builderType.addMethod(MethodSpec.methodBuilder(metadata.fieldName())
                    .addModifiers(Modifier.PUBLIC)
                    .returns(ClassName.get(packageName, factoryName, "Builder"))
                    .addParameter(metadata.typeName(), metadata.fieldName())
                    .addStatement("this.$N = $N", metadata.fieldName(), metadata.fieldName())
                    .addStatement("return this")
                    .build());
        }

        MethodSpec.Builder insertModeMethod = MethodSpec.methodBuilder("insert")
                .addModifiers(Modifier.PUBLIC)
                .returns(entityClass)
                .addParameter(insertMode, "mode")
                .addStatement("$T ctx = dataManager.createInsertContext()", insertContext);

        for (Metadata metadata : valueMetaData) {
            insertModeMethod.addStatement("ctx.set($N, $N, $N, this.$N)",
                    metadata.fieldName() + "$Schema",
                    metadata.fieldName() + "$Table",
                    metadata.fieldName() + "$Column",
                    metadata.fieldName());
        }
        insertModeMethod.addStatement("return ctx.insert(mode).get($T.class)", entityClass);

        builderType.addMethod(insertModeMethod.build());

        MethodSpec.Builder insertCtxMethod = MethodSpec.methodBuilder("insert")
                .addModifiers(Modifier.PUBLIC)
                .returns(TypeName.VOID)
                .addParameter(insertContext, "ctx");

        for (Metadata metadata : valueMetaData) {
            insertCtxMethod.addStatement("ctx.set($N, $N, $N, this.$N)",
                    metadata.fieldName() + "$Schema",
                    metadata.fieldName() + "$Table",
                    metadata.fieldName() + "$Column",
                    metadata.fieldName());
        }

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

    private List<Metadata> collectProperties(TypeElement type, TypeElement superType, Data dataAnnotation) {
        List<Metadata> meta = new ArrayList<>();
        for (VariableElement field : ElementFilter.fieldsIn(type.getEnclosedElements())) {
            if (field.getModifiers().contains(Modifier.STATIC)) continue;

            TypeMirror mirror = field.asType();
            if (processingEnv.getTypeUtils().isAssignable(processingEnv.getTypeUtils().erasure(mirror), superType.asType())) {
                if (mirror instanceof DeclaredType declared && declared.getTypeArguments().size() == 1) {
                    TypeMirror inner = declared.getTypeArguments().getFirst();
                    meta.add(getMetadata(field, TypeName.get(inner), dataAnnotation));
                }
            }
        }
        return meta;
    }

    private Metadata getMetadata(VariableElement field, TypeName typeName, Data dataAnnotation) {
        String schemaName = null;
        String tableName = null;
        String columnName = null;

        IdColumn idColumn = field.getAnnotation(IdColumn.class);
        Column column = field.getAnnotation(Column.class);
        ForeignColumn foreignColumn = field.getAnnotation(ForeignColumn.class);

        if (idColumn != null) {
            tableName = dataAnnotation.table();
            schemaName = dataAnnotation.schema();
            columnName = idColumn.name();
        } else if (column != null) {
            tableName = dataAnnotation.table();
            schemaName = dataAnnotation.schema();
            columnName = column.name();
        } else if (foreignColumn != null) {
            tableName = foreignColumn.table().isEmpty() ? dataAnnotation.table() : foreignColumn.table();
            schemaName = foreignColumn.schema().isEmpty() ? dataAnnotation.schema() : foreignColumn.schema();
            columnName = foreignColumn.name();
        }

        return new Metadata(
                schemaName,
                tableName,
                columnName,
                field.getSimpleName().toString(),
                typeName
        );
    }

    record Metadata(String schema, String table, String column, String fieldName, TypeName typeName) {
    }
}