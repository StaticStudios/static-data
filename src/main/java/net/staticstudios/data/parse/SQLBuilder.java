package net.staticstudios.data.parse;

import com.google.common.base.Preconditions;
import net.staticstudios.data.DataManager;
import net.staticstudios.data.Relation;
import net.staticstudios.data.UniqueData;
import net.staticstudios.data.util.*;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Field;
import java.util.*;

public class SQLBuilder {
    public static final String INDENT = "  ";
    private final Map<String, SQLSchema> parsedSchemas;
    private final DataManager dataManager;

    public SQLBuilder(DataManager dataManager) {
        this.dataManager = dataManager;
        this.parsedSchemas = new HashMap<>();
    }

    public List<String> parse(Class<? extends UniqueData> clazz) {
        Preconditions.checkNotNull(clazz, "Class cannot be null");

        Set<Class<? extends UniqueData>> visited = walk(clazz);
        Map<String, SQLSchema> schemas = new HashMap<>();
        for (Class<? extends UniqueData> visitedClass : visited) {
            parseIndividual(visitedClass, schemas);
        }


        for (SQLSchema newSchema : schemas.values()) {
            if (!this.parsedSchemas.containsKey(newSchema.getName())) {
                this.parsedSchemas.put(newSchema.getName(), newSchema);
                continue;
            }
            SQLSchema existingSchema = this.parsedSchemas.get(newSchema.getName());
            for (SQLTable newTable : newSchema.getTables()) {
                SQLTable existingTable = existingSchema.getTable(newTable.getName());
                if (existingTable == null) {
                    existingSchema.addTable(newTable);
                    continue;
                }
                for (SQLColumn newColumn : newTable.getColumns()) {
                    SQLColumn existingColumn = existingTable.getColumn(newColumn.getName());
                    if (existingColumn != null) {
                        Preconditions.checkState(existingColumn.equals(newColumn), "Column " + newColumn.getName() + " in table " + newTable.getName() + " has conflicting definitions! Existing: " + existingColumn + ", New: " + newColumn);
                        continue;
                    }
                    existingTable.addColumn(newColumn);
                }
            }
        }

        return getDefs(schemas.values());
    }

    public @Nullable SQLSchema getSchema(String name) {
        return parsedSchemas.get(name);
    }

    private List<String> getDefs(Collection<SQLSchema> schemas) {
        List<String> statements = new ArrayList<>();
        for (SQLSchema schema : schemas) {
            statements.add("CREATE SCHEMA IF NOT EXISTS \"" + schema.getName() + "\";");
            StringBuilder sb;
            for (SQLTable table : schema.getTables()) {
//                if (metadata.table().equals(table.getName()) && metadata.schema().equals(schema.getName())) {
                sb = new StringBuilder();
                sb.append("CREATE TABLE IF NOT EXISTS \"").append(schema.getName()).append("\".\"").append(table.getName()).append("\" (\n");
                for (ColumnMetadata idColumn : table.getIdColumns()) {
                    sb.append(INDENT).append("\"").append(idColumn.name()).append("\" ").append(SQLUtils.getSqlType(idColumn.type())).append(",\n");
                }
                sb.append(INDENT).append("PRIMARY KEY (");
                for (ColumnMetadata idColumn : table.getIdColumns()) {
                    sb.append("\"").append(idColumn.name()).append("\", ");
                }
                sb.setLength(sb.length() - 2);
                sb.append(")\n");
                sb.append(");");
                statements.add(sb.toString());
//                }
                if (!table.getColumns().isEmpty()) {
                    for (SQLColumn column : table.getColumns()) {
                        sb = new StringBuilder();
                        sb.append("ALTER TABLE \"").append(schema.getName()).append("\".\"").append(table.getName()).append("\" ").append("ADD COLUMN IF NOT EXISTS ").append("\"").append(column.getName()).append("\" ").append(SQLUtils.getSqlType(column.getType()));
//                        if (!column.isNullable()) {
//                            sb.append(" NOT NULL"); //todo: not valid in h2
//                        }
                        if (column.isIndexed()) {
//                            sb.append(" INDEXED");
                            //todo: this is not valid sql, need to create index separately
                        }
                        sb.append(";");
                        statements.add(sb.toString());
                    }
                }
            }
        }

        return statements;
    }

    private Set<Class<? extends UniqueData>> walk(Class<? extends UniqueData> clazz) {
        Preconditions.checkNotNull(clazz, "Class cannot be null");
        Preconditions.checkArgument(UniqueData.class.isAssignableFrom(clazz), "Class " + clazz.getName() + " is not a UniqueData type");

        Set<Class<? extends UniqueData>> visited = new HashSet<>();
        walk(clazz, visited);
        return visited;
    }

    private void walk(Class<? extends UniqueData> clazz, Set<Class<? extends UniqueData>> visited) {
        if (visited.contains(clazz)) {
            return;
        }
        visited.add(clazz);

        for (Field field : ReflectionUtils.getFields(clazz, Relation.class)) {
            if (Relation.class.isAssignableFrom(field.getType())) {
                Class<? extends UniqueData> related = Objects.requireNonNull(ReflectionUtils.getGenericType(field)).asSubclass(UniqueData.class);
                walk(related, visited);
            }
        }
    }

    private void parseIndividual(Class<? extends UniqueData> clazz, Map<String, SQLSchema> schemas) {
        UniqueDataMetadata metadata = dataManager.getMetadata(clazz);
        if (!clazz.isAnnotationPresent(Data.class)) {
            throw new IllegalArgumentException("Class " + clazz.getName() + " is not annotated with @Data");
        }

        Data dataAnnotation = clazz.getAnnotation(Data.class);
        Preconditions.checkNotNull(dataAnnotation, "Data annotation is null for class " + clazz.getName());

        for (Field field : ReflectionUtils.getFields(clazz)) {
            IdColumn idColumn = field.getAnnotation(IdColumn.class);
            Column columnAnnotation = field.getAnnotation(Column.class);
//            net.staticstudios.data.relation.Relation.OneToOne oneToOne = field.getAnnotation(net.staticstudios.data.relation.Relation.OneToOne.class);
            ForeignColumn foreignColumn = field.getAnnotation(ForeignColumn.class);
            //todo: when parsing a OneToOne relation, in the link if there is a column in our table that we have no already created, then we need to create it. note that the type should be the same as the referenced column type.
            //todo: add COLUMN REFERENCES bla bla bla for foreign keys
            ColumnMetadata columnMetadata = null;
            if (idColumn != null) {
                Preconditions.checkArgument(columnAnnotation == null, "PersistentValue field %s cannot be annotated with both @IdColumn and @Column", field.getName());
                columnMetadata = new ColumnMetadata(ValueUtils.parseValue(idColumn.name()), ReflectionUtils.getGenericType(field), false, false, ValueUtils.parseValue(dataAnnotation.table()), ValueUtils.parseValue(dataAnnotation.schema()));
            } else if (columnAnnotation != null) {
                columnMetadata = new ColumnMetadata(ValueUtils.parseValue(columnAnnotation.name()), ReflectionUtils.getGenericType(field), columnAnnotation.nullable(), columnAnnotation.index(), ValueUtils.parseValue(columnAnnotation.table()), ValueUtils.parseValue(columnAnnotation.schema()));
            } else if (foreignColumn != null) {
                columnMetadata = new ColumnMetadata(ValueUtils.parseValue(foreignColumn.name()), ReflectionUtils.getGenericType(field), foreignColumn.nullable(), foreignColumn.index(), ValueUtils.parseValue(foreignColumn.table()), ValueUtils.parseValue(foreignColumn.schema()));
            }
            if (columnMetadata == null) {
                continue;
            }

            String dataSchema = ValueUtils.parseValue(dataAnnotation.schema());
            String dataTable = ValueUtils.parseValue(dataAnnotation.table());

            String schemaName = columnMetadata.schema().isEmpty() ? dataSchema : columnMetadata.schema();
            String tableName = columnMetadata.table().isEmpty() ? dataTable : columnMetadata.table();
            String columnName = columnMetadata.name();

            if (foreignColumn != null) {
                Preconditions.checkArgument(!(schemaName.equals(dataSchema) && tableName.equals(dataTable)), "ForeignColumn field %s in class %s cannot reference its own table", field.getName(), clazz.getName());
            }

            SQLSchema schema = schemas.computeIfAbsent(schemaName, SQLSchema::new);
            SQLTable table = schema.getTable(tableName);
            if (table == null) {
                List<ColumnMetadata> idColumns = metadata.idColumns();

                if (foreignColumn != null) {
                    idColumns = new ArrayList<>();
                    List<String> links = StringUtils.parseCommaSeperatedList(foreignColumn.link());
                    for (String link : links) {
                        String[] parts = link.split("=");
                        Preconditions.checkArgument(parts.length == 2, "Invalid link format in OneToOne annotation on field %s in class %s. Expected format: localColumn=foreignColumn", field.getName(), clazz.getName());
                        String localColumn = ValueUtils.parseValue(parts[0].trim());
                        String otherColumn = ValueUtils.parseValue(parts[1].trim());

                        ColumnMetadata found = null;
                        for (ColumnMetadata idCol : metadata.idColumns()) {
                            if (idCol.name().equals(localColumn)) {
                                found = idCol;
                                break;
                            }
                        }
                        Preconditions.checkNotNull(found, "Link column %s in OneToOne annotation on field %s in class %s is not an ID column", localColumn, field.getName(), clazz.getName());

                        idColumns.add(new ColumnMetadata(otherColumn, found.type(), false, false, tableName, schemaName));
                    }
                }

                table = new SQLTable(schema, tableName, idColumns);
                schema.addTable(table);
            }

            Class<?> type = ReflectionUtils.getGenericType(field); //todo: handle custom types to sql types
            SQLColumn sqlColumn = new SQLColumn(table, type, columnName, columnMetadata.nullable(), columnMetadata.indexed());

            SQLColumn existingColumn = table.getColumn(columnName);
            if (existingColumn != null) {
                Preconditions.checkState(existingColumn.equals(sqlColumn), "Column " + columnName + " in table " + tableName + " has conflicting definitions! Existing: " + existingColumn + ", New: " + sqlColumn);
                continue;
            }

            table.addColumn(sqlColumn);
        }
    }
}
