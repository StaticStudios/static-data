package net.staticstudios.data.processor;

import com.palantir.javapoet.*;
import net.staticstudios.data.Data;

import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.PackageElement;
import javax.lang.model.element.TypeElement;
import java.io.IOException;
import java.util.List;

public class QueryFactory {
    private static final ClassName DATA_MANAGER_CLASS_NAME = ClassName.get("net.staticstudios.data", "DataManager");
    private static final ClassName ABSTRACT_QUERY_BUILDER_CLASS_NAME = ClassName.get("net.staticstudios.data.query", "AbstractQueryBuilder");
    private final ProcessingEnvironment processingEnv;
    private final TypeElement entityType;
    private final Data dataAnnotation;
    private final List<Metadata> metadataList;
    private final String entityName;
    private final String queryName;
    private final String packageName;
    private final ClassName entityClass;
    private final ClassName builderClassName;
    private final ClassName conditionalBuilderClassName;

    public QueryFactory(ProcessingEnvironment processingEnv, TypeElement entityType, Data dataAnnotation, List<Metadata> metadataList) {
        this.processingEnv = processingEnv;
        this.entityType = entityType;
        this.dataAnnotation = dataAnnotation;
        this.metadataList = metadataList;
        this.entityName = entityType.getSimpleName().toString();
        this.queryName = entityName + "Query";
        PackageElement packageElement = processingEnv.getElementUtils().getPackageOf(entityType);
        this.packageName = packageElement.isUnnamed() ? "" : packageElement.getQualifiedName().toString();
        this.entityClass = ClassName.get(packageName, entityName);
        this.builderClassName = ClassName.get(packageName, queryName, "Builder");
        this.conditionalBuilderClassName = ClassName.get(packageName, queryName, "ConditionalBuilder");
    }

    public void generateQueryBuilder() throws IOException {
        TypeSpec.Builder queryBuilder = TypeSpec.classBuilder(queryName);
        TypeSpec.Builder conditionalBuilderType = TypeSpec.classBuilder("ConditionalBuilder")
                .superclass(ParameterizedTypeName.get(ClassName.get("net.staticstudios.data.query", "AbstractConditionalBuilder"), builderClassName, conditionalBuilderClassName, entityClass))
                .addModifiers(Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
                .addMethod(MethodSpec.constructorBuilder()
                        .addParameter(ParameterizedTypeName.get(ABSTRACT_QUERY_BUILDER_CLASS_NAME, builderClassName, conditionalBuilderClassName, entityClass), "queryBuilder")
                        .addStatement("super(queryBuilder)")
                        .build());

        TypeSpec.Builder builderType = TypeSpec.classBuilder("Builder")
                .superclass(ParameterizedTypeName.get(ABSTRACT_QUERY_BUILDER_CLASS_NAME, builderClassName, conditionalBuilderClassName, entityClass))
                .addModifiers(Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
                .addMethod(MethodSpec.constructorBuilder()
                        .addParameter(DATA_MANAGER_CLASS_NAME, "dataManager")
                        .addStatement("super(dataManager, $N.class)", entityClass.simpleName())
                        .build())
                .addMethod(MethodSpec.methodBuilder("createInstance")
                        .addModifiers(Modifier.PROTECTED)
                        .returns(ParameterizedTypeName.get(ABSTRACT_QUERY_BUILDER_CLASS_NAME, builderClassName, conditionalBuilderClassName, entityClass))
                        .addStatement("return new Builder(this.dataManager)")
                        .build())
                .addMethod(MethodSpec.methodBuilder("createConditionalInstance")
                        .addModifiers(Modifier.PROTECTED)
                        .returns(conditionalBuilderClassName)
                        .addStatement("return new ConditionalBuilder(this)")
                        .build());

        for (Metadata metadata : metadataList) {
            if (metadata instanceof PersistentValueMetadata persistentValueMetadata) {
                if (metadata instanceof ForeignPersistentValueMetadata) {
                    continue; //todo: for now we dont support these queries. we will need to tho and integrate joins
                }
                SchemaTableColumnStatics.generateSchemaTableColumnStatics(queryBuilder, persistentValueMetadata);
                makeEqualsClause(builderType, persistentValueMetadata);
                makeNotEqualsClause(builderType, persistentValueMetadata);
                makeInClause(builderType, persistentValueMetadata);
                makeNotInClause(builderType, persistentValueMetadata);

                if (persistentValueMetadata.nullable()) {
                    makeNullClause(builderType, persistentValueMetadata);
                    makeNotNullClause(builderType, persistentValueMetadata);
                }

                if (TypeName.FLOAT.box().equals(persistentValueMetadata.genericType()) //todo: support timestampts and dates
                        || TypeName.DOUBLE.box().equals(persistentValueMetadata.genericType())
                        || TypeName.LONG.box().equals(persistentValueMetadata.genericType())
                        || TypeName.SHORT.box().equals(persistentValueMetadata.genericType())
                        || TypeName.BYTE.box().equals(persistentValueMetadata.genericType())
                        || TypeName.INT.box().equals(persistentValueMetadata.genericType())
                ) {
                    makeLessThanClause(builderType, persistentValueMetadata);
                    makeLessThanOrEqualToClause(builderType, persistentValueMetadata);
                    makeGreaterThanClause(builderType, persistentValueMetadata);
                    makeGreaterThanOrEqualToClause(builderType, persistentValueMetadata);
                    makeBetweenClause(builderType, persistentValueMetadata);
                }

                if (TypeName.get(String.class).equals(persistentValueMetadata.genericType())) {
                    makeLikeClause(builderType, persistentValueMetadata);
                    makeNotLikeClause(builderType, persistentValueMetadata);
                }

                makeOrderByClause(builderType, persistentValueMetadata, builderClassName);
                makeOrderByClause(conditionalBuilderType, persistentValueMetadata, conditionalBuilderClassName);
            }
        }

        TypeSpec query = queryBuilder
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addMethod(MethodSpec.constructorBuilder()
                        .addModifiers(Modifier.PRIVATE)
                        .build())
                .addMethod(MethodSpec.methodBuilder("where")
                        .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
                        .returns(ClassName.get(packageName, queryName, "Builder"))
                        .addParameter(DATA_MANAGER_CLASS_NAME, "dataManager")
                        .addStatement("return new Builder(dataManager)")
                        .build())
                .addMethod(MethodSpec.methodBuilder("where")
                        .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
                        .returns(ClassName.get(packageName, queryName, "Builder"))
                        .addStatement("return new Builder(DataManager.getInstance())")
                        .build())
                .addType(builderType.build())
                .addType(conditionalBuilderType.build())
                .build();

        JavaFile.builder(packageName, query)
                .indent("    ")
                .build()
                .writeTo(processingEnv.getFiler());
    }


    private void makeNotEqualsClause(TypeSpec.Builder builderType, PersistentValueMetadata persistentValueMetadata) {
        makeClause(builderType, persistentValueMetadata, ClassName.get("net.staticstudios.data.query.clause", "NotEqualsClause"), "IsNot");
    }

    private void makeEqualsClause(TypeSpec.Builder builderType, PersistentValueMetadata persistentValueMetadata) {
        makeClause(builderType, persistentValueMetadata, ClassName.get("net.staticstudios.data.query.clause", "EqualsClause"), "Is");
    }

    private void makeInClause(TypeSpec.Builder builderType, PersistentValueMetadata persistentValueMetadata) {
        makeClause(builderType, persistentValueMetadata, ClassName.get("net.staticstudios.data.query.clause", "InClause"), "IsIn", true);
        makeClause(builderType, persistentValueMetadata, ClassName.get("net.staticstudios.data.query.clause", "InClause"), "IsIn", false, ParameterizedTypeName.get(ClassName.get(List.class), persistentValueMetadata.genericType()));
    }

    private void makeNotInClause(TypeSpec.Builder builderType, PersistentValueMetadata persistentValueMetadata) {
        makeClause(builderType, persistentValueMetadata, ClassName.get("net.staticstudios.data.query.clause", "NotInClause"), "IsNotIn", true);
        makeClause(builderType, persistentValueMetadata, ClassName.get("net.staticstudios.data.query.clause", "NotInClause"), "IsNotIn", false, ParameterizedTypeName.get(ClassName.get(List.class), persistentValueMetadata.genericType()));
    }

    private void makeLessThanClause(TypeSpec.Builder builderType, PersistentValueMetadata persistentValueMetadata) {
        makeClause(builderType, persistentValueMetadata,
                ParameterizedTypeName.get(ClassName.get("net.staticstudios.data.query.clause", "LessThanClause"), persistentValueMetadata.genericType()),
                "IsLessThan");
    }

    private void makeLessThanOrEqualToClause(TypeSpec.Builder builderType, PersistentValueMetadata persistentValueMetadata) {
        makeClause(builderType, persistentValueMetadata,
                ParameterizedTypeName.get(ClassName.get("net.staticstudios.data.query.clause", "LessThanOrEqualToClause"), persistentValueMetadata.genericType()),
                "IsLessThanOrEqualTo");
    }

    private void makeGreaterThanClause(TypeSpec.Builder builderType, PersistentValueMetadata persistentValueMetadata) {
        makeClause(builderType, persistentValueMetadata,
                ParameterizedTypeName.get(ClassName.get("net.staticstudios.data.query.clause", "GreaterThanClause"), persistentValueMetadata.genericType()),
                "IsGreaterThan");
    }

    private void makeGreaterThanOrEqualToClause(TypeSpec.Builder builderType, PersistentValueMetadata persistentValueMetadata) {
        makeClause(builderType, persistentValueMetadata,
                ParameterizedTypeName.get(ClassName.get("net.staticstudios.data.query.clause", "GreaterThanOrEqualToClause"), persistentValueMetadata.genericType()),
                "IsGreaterThanOrEqualTo");
    }

    private void makeNullClause(TypeSpec.Builder builderType, PersistentValueMetadata persistentValueMetadata) {
        makeNonValuedClause(builderType, persistentValueMetadata, ClassName.get("net.staticstudios.data.query.clause", "NullClause"), "IsNull");
    }

    private void makeNotNullClause(TypeSpec.Builder builderType, PersistentValueMetadata persistentValueMetadata) {
        makeNonValuedClause(builderType, persistentValueMetadata, ClassName.get("net.staticstudios.data.query.clause", "NotNullClause"), "IsNotNull");
    }

    private void makeLikeClause(TypeSpec.Builder builderType, PersistentValueMetadata persistentValueMetadata) {
        makeClause(builderType, persistentValueMetadata, ClassName.get("net.staticstudios.data.query.clause", "LikeClause"), "IsLike");
    }

    private void makeNotLikeClause(TypeSpec.Builder builderType, PersistentValueMetadata persistentValueMetadata) {
        makeClause(builderType, persistentValueMetadata, ClassName.get("net.staticstudios.data.query.clause", "NotLikeClause"), "IsNotLike");
    }

    private void makeBetweenClause(TypeSpec.Builder builderType, PersistentValueMetadata persistentValueMetadata) {
        builderType.addMethod(MethodSpec.methodBuilder(persistentValueMetadata.fieldName() + "IsBetween")
                .addModifiers(Modifier.PUBLIC)
                .returns(conditionalBuilderClassName)
                .addParameter(persistentValueMetadata.genericType(), "start")
                .addParameter(persistentValueMetadata.genericType(), "end")
                .addStatement("return set(new $T($N, $N, $N, start, end))",
                        ParameterizedTypeName.get(ClassName.get("net.staticstudios.data.query.clause", "BetweenClause"), persistentValueMetadata.genericType()),
                        persistentValueMetadata.fieldName() + "$schema",
                        persistentValueMetadata.fieldName() + "$table",
                        persistentValueMetadata.fieldName() + "$column"
                )
                .build());
    }

    private void makeClause(TypeSpec.Builder builderType, PersistentValueMetadata persistentValueMetadata, TypeName clauseTypeName, String suffix) {
        makeClause(builderType, persistentValueMetadata, clauseTypeName, suffix, false);
    }

    private void makeClause(TypeSpec.Builder builderType, PersistentValueMetadata persistentValueMetadata, TypeName clauseTypeName, String suffix, boolean varargs) {
        makeClause(builderType, persistentValueMetadata, clauseTypeName, suffix, varargs, persistentValueMetadata.genericType());
    }

    private void makeClause(TypeSpec.Builder builderType, PersistentValueMetadata persistentValueMetadata, TypeName clauseTypeName, String suffix, boolean varargs, TypeName parameterType) {
        builderType.addMethod(MethodSpec.methodBuilder(persistentValueMetadata.fieldName() + suffix)
                .addModifiers(Modifier.PUBLIC)
                .returns(conditionalBuilderClassName)
                .addParameter(varargs ? ArrayTypeName.of(parameterType) : parameterType, persistentValueMetadata.fieldName())
                .varargs(varargs)
                .addStatement("return set(new $T($N, $N, $N, $N))",
                        clauseTypeName,
                        persistentValueMetadata.fieldName() + "$schema",
                        persistentValueMetadata.fieldName() + "$table",
                        persistentValueMetadata.fieldName() + "$column",
                        persistentValueMetadata.fieldName())
                .build());
    }

    private void makeNonValuedClause(TypeSpec.Builder builderType, PersistentValueMetadata persistentValueMetadata, TypeName clauseTypeName, String suffix) {
        builderType.addMethod(MethodSpec.methodBuilder(persistentValueMetadata.fieldName() + suffix)
                .addModifiers(Modifier.PUBLIC)
                .returns(conditionalBuilderClassName)
                .addStatement("return set(new $T($N, $N, $N))",
                        clauseTypeName,
                        persistentValueMetadata.fieldName() + "$schema",
                        persistentValueMetadata.fieldName() + "$table",
                        persistentValueMetadata.fieldName() + "$column"
                )
                .build());
    }

    private void makeOrderByClause(TypeSpec.Builder builderType, PersistentValueMetadata persistentValueMetadata, ClassName returnType) {
        String methodName = "orderBy" + persistentValueMetadata.fieldName().substring(0, 1).toUpperCase() + persistentValueMetadata.fieldName().substring(1);
        builderType.addMethod(MethodSpec.methodBuilder(methodName)
                .addModifiers(Modifier.PUBLIC)
                .returns(returnType)
                .addParameter(ClassName.get("net.staticstudios.data.query", "Order"), "order")
                .addStatement("orderBy($N, $N, $N, order)",
                        persistentValueMetadata.fieldName() + "$schema",
                        persistentValueMetadata.fieldName() + "$table",
                        persistentValueMetadata.fieldName() + "$column"
                )
                .addStatement("return this")
                .build());
    }
}
