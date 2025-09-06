package net.staticstudios.data.parse;

import com.google.common.base.Preconditions;
import net.staticstudios.data.Relation;
import net.staticstudios.data.UniqueData;
import net.staticstudios.data.util.ReflectionUtils;
import net.staticstudios.data.util.ValueUtils;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class SQLBuilder {
    private final Map<String, SQLSchema> schemas;

    public SQLBuilder() {
        this.schemas = new HashMap<>();
    }

    public void parse(Class<? extends UniqueData> clazz) {
        Preconditions.checkNotNull(clazz, "Class cannot be null");

        Set<Class<? extends UniqueData>> visited = walk(clazz);
        for (Class<? extends UniqueData> visitedClass : visited) {
            parseIndividual(visitedClass);
        }
    }

    public String asSQL() { //todo: create table if not exist then use alter statements
        StringBuilder sb = new StringBuilder();
        for (SQLSchema schema : schemas.values()) {
            sb.append("CREATE SCHEMA IF NOT EXISTS ").append(schema.getName()).append(";\n");
            for (SQLTable table : schema.getTables()) {
                sb.append("CREATE TABLE IF NOT EXISTS ").append(schema.getName()).append(".").append(table.getName()).append(" (\n");
                for (SQLColumn column : table.getColumns()) {
                    sb.append("  ").append(column.getName()).append(" ?????").append(column.isNullable() ? "" : " NOT NULL");
                    if (column.isIndexed()) {
                        sb.append(" INDEXED");
                    }
                    sb.append(",\n");
                }
                sb.setLength(sb.length() - 2);
                sb.append("\n);\n");
            }
        }

        return sb.toString();
    }

    private Set<Class<? extends UniqueData>> walk(Class<? extends UniqueData> clazz) {
        Preconditions.checkNotNull(clazz, "Class cannot be null");
        Preconditions.checkArgument(UniqueData.class.isAssignableFrom(clazz), "Class " + clazz.getName() + " is not a UniqueData type");

        Set<Class<? extends UniqueData>> visited = new java.util.HashSet<>();
        walk(clazz, visited);
        return visited;
    }

    private void walk(Class<? extends UniqueData> clazz, Set<Class<? extends UniqueData>> visited) {
        if (visited.contains(clazz)) {
            return;
        }
        visited.add(clazz);

        for (Field field : ReflectionUtils.getFields(clazz)) {
            if (Relation.class.isAssignableFrom(field.getType())) {
                Class<?> genericType = ReflectionUtils.getGenericType(field);
                Preconditions.checkNotNull(genericType, "Generic type for field " + field.getName() + " in class " + clazz.getName() + " is null");
                Preconditions.checkArgument(UniqueData.class.isAssignableFrom(genericType), "Field " + field.getName() + " in class " + clazz.getName() + " is not a UniqueData type");
                Class<? extends UniqueData> relatedClass = (Class<? extends UniqueData>) genericType;

                walk(relatedClass, visited);
            }
        }
    }

    private void parseIndividual(Class<? extends UniqueData> clazz) {
        if (!clazz.isAnnotationPresent(Data.class)) {
            throw new IllegalArgumentException("Class " + clazz.getName() + " is not annotated with @Data");
        }

        Data dataAnnotation = clazz.getAnnotation(Data.class);
        Preconditions.checkNotNull(dataAnnotation, "Data annotation is null for class " + clazz.getName());


        for (Field field : ReflectionUtils.getFields(clazz)) {
            if (!field.isAnnotationPresent(Column.class)) {
                continue;
            }

            Column column = field.getAnnotation(Column.class);
            Preconditions.checkNotNull(column, "Column annotation is null for field " + field.getName() + " in class " + clazz.getName());
            String schemaName = column.schema().isEmpty() ? ValueUtils.parseValue(dataAnnotation.schema()) : ValueUtils.parseValue(column.schema());
            String tableName = column.table().isEmpty() ? ValueUtils.parseValue(dataAnnotation.table()) : ValueUtils.parseValue(column.table());
            String columnName = ValueUtils.parseValue(column.value());

            SQLSchema schema = schemas.computeIfAbsent(schemaName, SQLSchema::new);
            SQLTable table = schema.getTable(tableName);
            if (table == null) {
                table = new SQLTable(schema, tableName);
                schema.addTable(table);
            }

            //todo: grab the type of the PV to determine the SQL type
            SQLColumn sqlColumn = new SQLColumn(table, columnName, column.nullable(), column.index());
            table.addColumn(sqlColumn);
        }
    }
}
