package net.staticstudios.data.parse;

import com.google.common.base.Preconditions;
import net.staticstudios.data.*;
import net.staticstudios.data.util.*;
import org.intellij.lang.annotations.Language;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.*;

public class SQLBuilder {
    public static final String INDENT = "  ";
    private static final Logger logger = LoggerFactory.getLogger(SQLBuilder.class);
    private final Map<String, SQLSchema> parsedSchemas;
    private final DataManager dataManager;

    public SQLBuilder(DataManager dataManager) {
        this.dataManager = dataManager;
        this.parsedSchemas = new HashMap<>();
    }

    public List<DDLStatement> parse(Class<? extends UniqueData> clazz) {
        Preconditions.checkNotNull(clazz, "Class cannot be null");

        Set<Class<? extends UniqueData>> visited = walk(clazz);
        Map<String, SQLSchema> schemas = new HashMap<>();
        for (Class<? extends UniqueData> visitedClass : visited) {
            parseIndividualColumns(visitedClass, schemas);
        }
        for (Class<? extends UniqueData> visitedClass : visited) {
            parseIndividualRelations(visitedClass, schemas);
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

    private List<DDLStatement> getDefs(Collection<SQLSchema> schemas) {
        List<DDLStatement> statements = new ArrayList<>();
        for (SQLSchema schema : schemas) {
            statements.add(DDLStatement.both("CREATE SCHEMA IF NOT EXISTS \"" + schema.getName() + "\";"));
            StringBuilder h2Sb;
            StringBuilder pgSb;
            for (SQLTable table : schema.getTables()) {
                h2Sb = new StringBuilder();
                pgSb = new StringBuilder();
                h2Sb.append("CREATE TABLE IF NOT EXISTS \"").append(schema.getName()).append("\".\"").append(table.getName()).append("\" (\n");
                pgSb.append("CREATE TABLE IF NOT EXISTS \"").append(schema.getName()).append("\".\"").append(table.getName()).append("\" (\n");
                for (ColumnMetadata idColumn : table.getIdColumns()) {
                    h2Sb.append(INDENT).append("\"").append(idColumn.name()).append("\" ").append(SQLUtils.getH2SqlType(idColumn.type())).append(",\n");
                    pgSb.append(INDENT).append("\"").append(idColumn.name()).append("\" ").append(SQLUtils.getPgSqlType(idColumn.type())).append(" NOT NULL,\n");
                }
                h2Sb.append(INDENT).append("PRIMARY KEY (");
                pgSb.append(INDENT).append("PRIMARY KEY (");
                for (ColumnMetadata idColumn : table.getIdColumns()) {
                    h2Sb.append("\"").append(idColumn.name()).append("\", ");
                    pgSb.append("\"").append(idColumn.name()).append("\", ");
                }
                h2Sb.setLength(h2Sb.length() - 2);
                pgSb.setLength(pgSb.length() - 2);
                h2Sb.append(")\n");
                pgSb.append(")\n");
                h2Sb.append(");");
                pgSb.append(");");
                statements.add(DDLStatement.of(h2Sb.toString(), pgSb.toString()));
                if (!table.getColumns().isEmpty()) {
                    for (SQLColumn column : table.getColumns()) {
                        h2Sb = new StringBuilder();
                        pgSb = new StringBuilder();
                        h2Sb.append("ALTER TABLE \"").append(schema.getName()).append("\".\"").append(table.getName()).append("\" ").append("ADD COLUMN IF NOT EXISTS ").append("\"").append(column.getName()).append("\" ").append(SQLUtils.getH2SqlType(column.getType()));
                        pgSb.append("ALTER TABLE \"").append(schema.getName()).append("\".\"").append(table.getName()).append("\" ").append("ADD COLUMN IF NOT EXISTS ").append("\"").append(column.getName()).append("\" ").append(SQLUtils.getPgSqlType(column.getType()));
                        if (!column.isNullable()) {
                            h2Sb.append(" NOT NULL");
                            pgSb.append(" NOT NULL");
                        }
                        if (column.getDefaultValue() != null) {
                            h2Sb.append(" DEFAULT ").append(column.getDefaultValue());
                            pgSb.append(" DEFAULT ").append(column.getDefaultValue());
                        }

                        if (column.isUnique()) {
                            h2Sb.append(" UNIQUE");
                            pgSb.append(" UNIQUE");
                        }

                        h2Sb.append(";");
                        pgSb.append(";");
                        statements.add(DDLStatement.of(h2Sb.toString(), pgSb.toString()));
                    }
                }
            }
        }

        for (SQLSchema schema : schemas) {
            for (SQLTable table : schema.getTables()) {
                for (SQLColumn column : table.getColumns()) {
                    if (column.isIndexed() && !column.isUnique()) {
                        String indexName = "idx_" + schema.getName() + "_" + table.getName() + "_" + column.getName();
                        @Language("SQL") String h2 = "CREATE INDEX IF NOT EXISTS " + indexName + " ON \"" + schema.getName() + "\".\"" + table.getName() + "\" (\"" + column.getName() + "\");";
                        statements.add(DDLStatement.both(h2));
                    }
                }
            }
        }

        // define fkeys after table creation, to ensure all tables exist before adding fkeys
        for (SQLSchema schema : schemas) { //todo: what if an fkey's insert/delete strategy has changed?
            for (SQLTable table : schema.getTables()) {
                for (ForeignKey foreignKey : table.getForeignKeys()) {
                    if (foreignKey == null) {
                        continue;
                    }
                    String fKeyName = "fk_" + foreignKey.getReferringSchema() + "_" + foreignKey.getReferringTable() + "_"
                            + String.join("_", foreignKey.getLinkingColumns().stream().map(ForeignKey.Link::columnInReferringTable).toList())
                            + "_to_" + foreignKey.getReferencedSchema() + "_" + foreignKey.getReferencedTable() + "_" + String.join("_", foreignKey.getLinkingColumns().stream().map(ForeignKey.Link::columnInReferencedTable).toList());
                    StringBuilder sb = new StringBuilder();
                    sb.append("ALTER TABLE \"").append(foreignKey.getReferringSchema()).append("\".\"").append(foreignKey.getReferringTable()).append("\" ");
                    sb.append("ADD CONSTRAINT IF NOT EXISTS ").append(fKeyName).append(" ");
                    sb.append("FOREIGN KEY (");
                    for (ForeignKey.Link link : foreignKey.getLinkingColumns()) {
                        sb.append("\"").append(link.columnInReferringTable()).append("\", ");
                    }
                    sb.setLength(sb.length() - 2);
                    sb.append(") ");
                    sb.append("REFERENCES \"").append(foreignKey.getReferencedSchema()).append("\".\"").append(foreignKey.getReferencedTable()).append("\" (");
                    for (ForeignKey.Link link : foreignKey.getLinkingColumns()) {
                        sb.append("\"").append(link.columnInReferencedTable()).append("\", ");
                    }
                    sb.setLength(sb.length() - 2);
                    sb.append(") ON DELETE ").append(foreignKey.getOnDelete()).append(" ON UPDATE ").append(foreignKey.getOnUpdate()).append(";");
                    String h2 = sb.toString();


                    sb = new StringBuilder();
                    sb.append("DO $$ BEGIN ");
                    sb.append("IF NOT EXISTS (SELECT 1 FROM information_schema.table_constraints WHERE constraint_name = '").append(fKeyName).append("' AND table_name = '").append(foreignKey.getReferringTable()).append("' AND constraint_schema = '").append(foreignKey.getReferringSchema()).append("' AND constraint_type = 'FOREIGN KEY') THEN ");

                    sb.append("ALTER TABLE \"").append(foreignKey.getReferringSchema()).append("\".\"").append(foreignKey.getReferringTable()).append("\" ");
                    sb.append("ADD CONSTRAINT ").append(fKeyName).append(" ");
                    sb.append("FOREIGN KEY (");
                    for (ForeignKey.Link link : foreignKey.getLinkingColumns()) {
                        sb.append("\"").append(link.columnInReferringTable()).append("\", ");
                    }
                    sb.setLength(sb.length() - 2);
                    sb.append(") ");
                    sb.append("REFERENCES \"").append(foreignKey.getReferencedSchema()).append("\".\"").append(foreignKey.getReferencedTable()).append("\" (");
                    for (ForeignKey.Link link : foreignKey.getLinkingColumns()) {
                        sb.append("\"").append(link.columnInReferencedTable()).append("\", ");
                    }
                    sb.setLength(sb.length() - 2);
                    sb.append(") ON DELETE ").append(foreignKey.getOnDelete()).append(" ON UPDATE ").append(foreignKey.getOnUpdate()).append(";");
                    sb.append(" END IF; END $$;");
                    String pg = sb.toString();
                    statements.add(DDLStatement.of(h2, pg));
                }
            }
        }
        for (SQLSchema schema : schemas) {
            for (SQLTable table : schema.getTables()) {
                for (SQLTrigger trigger : table.getTriggers()) {
                    statements.add(DDLStatement.of(trigger.getH2SQL(), trigger.getPgSQL()));
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

    private void parseIndividualColumns(Class<? extends UniqueData> clazz, Map<String, SQLSchema> schemas) {
        logger.trace("Parsing columns for class {}", clazz.getName());
        UniqueDataMetadata metadata = dataManager.getMetadata(clazz);
        if (!clazz.isAnnotationPresent(Data.class)) {
            throw new IllegalArgumentException("Class " + clazz.getName() + " is not annotated with @Data");
        }

        Data dataAnnotation = clazz.getAnnotation(Data.class);
        Preconditions.checkNotNull(dataAnnotation, "Data annotation is null for class " + clazz.getName());

        for (Field field : ReflectionUtils.getFields(clazz)) {
            parseColumn(clazz, schemas, dataAnnotation, metadata, field);
        }
    }

    private void parseIndividualRelations(Class<? extends UniqueData> clazz, Map<String, SQLSchema> schemas) {
        logger.trace("Parsing relations for class {}", clazz.getName());
        UniqueDataMetadata metadata = dataManager.getMetadata(clazz);
        if (!clazz.isAnnotationPresent(Data.class)) {
            throw new IllegalArgumentException("Class " + clazz.getName() + " is not annotated with @Data");
        }

        Data dataAnnotation = clazz.getAnnotation(Data.class);
        Preconditions.checkNotNull(dataAnnotation, "Data annotation is null for class " + clazz.getName());

        for (Field field : ReflectionUtils.getFields(clazz)) {
            parseReference(clazz, schemas, dataAnnotation, metadata, field);
        }
    }

    private void parseColumn(Class<? extends UniqueData> clazz, Map<String, SQLSchema> schemas, Data dataAnnotation, UniqueDataMetadata metadata, Field field) {
        if (!field.getType().equals(PersistentValue.class)) {
            return;
        }

        IdColumn idColumn = field.getAnnotation(IdColumn.class);
        Column columnAnnotation = field.getAnnotation(Column.class);
        ForeignColumn foreignColumn = field.getAnnotation(ForeignColumn.class);
        DefaultValue defaultValueAnnotation = field.getAnnotation(DefaultValue.class);

        int annotationsCount = 0;
        if (idColumn != null) annotationsCount++;
        if (columnAnnotation != null) annotationsCount++;
        if (foreignColumn != null) annotationsCount++;
        Preconditions.checkArgument(annotationsCount <= 1, "Field " + field.getName() + " in class " + clazz.getName() + " has multiple column annotations. Only one of @IdColumn, @Column, or @ForeignColumn is allowed.");

        String schemaName;
        String tableName;
        String columnName;
        boolean nullable;
        boolean indexed;
        boolean unique;
        String defaultValue = defaultValueAnnotation != null ? ValueUtils.parseValue(defaultValueAnnotation.value()) : "";
        if (idColumn != null) {
            schemaName = ValueUtils.parseValue(dataAnnotation.schema());
            tableName = ValueUtils.parseValue(dataAnnotation.table());
            columnName = ValueUtils.parseValue(idColumn.name());
            nullable = false;
            indexed = false;
            unique = true;
            defaultValue = "";
        } else if (columnAnnotation != null) {
            schemaName = ValueUtils.parseValue(columnAnnotation.schema());
            tableName = ValueUtils.parseValue(columnAnnotation.table());
            columnName = ValueUtils.parseValue(columnAnnotation.name());
            nullable = columnAnnotation.nullable();
            indexed = columnAnnotation.index();
            unique = columnAnnotation.unique();
        } else if (foreignColumn != null) {
            schemaName = ValueUtils.parseValue(foreignColumn.schema());
            tableName = ValueUtils.parseValue(foreignColumn.table());
            columnName = ValueUtils.parseValue(foreignColumn.name());
            nullable = foreignColumn.nullable();
            indexed = foreignColumn.index();
            unique = false;
        } else {
            return;
        }

        String dataSchema = ValueUtils.parseValue(dataAnnotation.schema());
        String dataTable = ValueUtils.parseValue(dataAnnotation.table());

        schemaName = schemaName.isEmpty() ? dataSchema : schemaName;
        tableName = tableName.isEmpty() ? dataTable : tableName;

        if (foreignColumn != null) {
            Preconditions.checkArgument(!(schemaName.equals(dataSchema) && tableName.equals(dataTable)), "ForeignColumn field %s in class %s cannot reference its own table", field.getName(), clazz.getName());
        }

        SQLSchema schema = schemas.computeIfAbsent(schemaName, SQLSchema::new);
        SQLTable table = schema.getTable(tableName);
        if (table == null) {
            List<ColumnMetadata> idColumns;

            if (foreignColumn == null) {
                idColumns = metadata.idColumns();
            } else {
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
                    Preconditions.checkNotNull(found, "Link name %s in OneToOne annotation on field %s in class %s is not an ID name", localColumn, field.getName(), clazz.getName());

                    idColumns.add(new ColumnMetadata(schemaName, tableName, otherColumn, found.type(), false, false, ""));
                }
            }

            table = new SQLTable(schema, tableName, idColumns);
            schema.addTable(table);

            if (foreignColumn != null) {
                for (ColumnMetadata idCol : table.getIdColumns()) {
                    Preconditions.checkState(table.getColumn(idCol.name()) == null, "ID column name " + idCol.name() + " in table " + tableName + " is duplicated!");
                    SQLColumn sqlColumn = new SQLColumn(table, idCol.type(), idCol.name(), false, false, true, null);
                    table.addColumn(sqlColumn);
                }
            }
        } else if (foreignColumn != null) {
            List<ForeignKey.Link> links = parseLinks(foreignColumn.link());
            for (ForeignKey.Link link : links) {
                ColumnMetadata found = null;
                for (ColumnMetadata idCol : metadata.idColumns()) {
                    if (idCol.name().equals(link.columnInReferringTable())) {
                        found = idCol;
                        break;
                    }
                }
                Preconditions.checkNotNull(found, "Link name %s in ForeignColumn annotation on field %s in class %s is not an ID name", link.columnInReferringTable(), field.getName(), clazz.getName());
            }
        }

        if (foreignColumn != null) {
            SQLSchema dataSqlSchema = schemas.computeIfAbsent(dataSchema, SQLSchema::new);
            SQLTable dataSqlTable = dataSqlSchema.getTable(dataTable);
            if (dataSqlTable == null) {
                dataSqlTable = new SQLTable(dataSqlSchema, dataTable, metadata.idColumns());
                dataSqlSchema.addTable(dataSqlTable);
            }

            String referencedSchema = ValueUtils.parseValue(foreignColumn.schema());
            if (referencedSchema.isEmpty()) {
                referencedSchema = schemaName;
            }
            String referencedTable = ValueUtils.parseValue(foreignColumn.table());
            if (referencedTable.isEmpty()) {
                referencedTable = tableName;
            }

            Preconditions.checkArgument(!(referencedSchema.equals(dataSchema) && referencedTable.equals(dataTable)), "ForeignColumn field %s in class %s cannot reference its own table", field.getName(), clazz.getName());

            ForeignKey foreignKey = new ForeignKey(dataSchema, dataTable, referencedSchema, referencedTable, OnDelete.CASCADE, OnUpdate.CASCADE);
            try {
                parseLinks(foreignKey, foreignColumn.link());
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Error parsing @ForeignColumn link on field " + field.getName() + " in class " + clazz.getName() + ": " + e.getMessage(), e);
            }
            dataSqlTable.addForeignKey(foreignKey);

            Delete delete = field.getAnnotation(Delete.class);
            DeleteStrategy deleteStrategy = delete != null ? delete.value() : DeleteStrategy.NO_ACTION;
            dataSqlTable.addTrigger(new SQLDeleteStrategyTrigger(dataSchema, dataTable, referencedSchema, referencedTable, deleteStrategy, foreignKey.getLinkingColumns()));
        }

        Class<?> type = dataManager.getSerializedType(ReflectionUtils.getGenericType(field));
        SQLColumn sqlColumn = new SQLColumn(table, type, columnName, nullable, indexed, unique, defaultValue.isEmpty() ? null : SQLUtils.parseDefaultValue(type, defaultValue));

        SQLColumn existingColumn = table.getColumn(columnName);
        if (existingColumn != null) {
            Preconditions.checkState(existingColumn.equals(sqlColumn), "Column " + columnName + " in table " + tableName + " has conflicting definitions! Existing: " + existingColumn + ", New: " + sqlColumn);
            return;
        }

        table.addColumn(sqlColumn);
    }

    private void parseReference(Class<? extends UniqueData> clazz, Map<String, SQLSchema> schemas, Data dataAnnotation, UniqueDataMetadata metadata, Field field) {
        if (!field.getType().equals(Reference.class)) {
            return;
        }
        Class<?> genericType = ReflectionUtils.getGenericType(field);
        Preconditions.checkArgument(genericType != null && UniqueData.class.isAssignableFrom(genericType), "Field " + field.getName() + " in class " + clazz.getName() + " is not parameterized with a UniqueData type! Generic type: " + genericType);
        UniqueDataMetadata referencedMetadata = dataManager.getMetadata(genericType.asSubclass(UniqueData.class));
        Preconditions.checkNotNull(referencedMetadata, "No metadata found for referenced class " + genericType.getName());
        OneToOne oneToOne = field.getAnnotation(OneToOne.class);
        if (oneToOne == null) {
            return;
        }

        String dataSchema = ValueUtils.parseValue(dataAnnotation.schema());
        String dataTable = ValueUtils.parseValue(dataAnnotation.table());

        SQLSchema schema = schemas.computeIfAbsent(dataSchema, SQLSchema::new);
        SQLTable table = schema.getTable(dataTable);

        if (table == null) {
            table = new SQLTable(schema, dataTable, metadata.idColumns());
            schema.addTable(table);

        }

        SQLSchema referencedSchema = Objects.requireNonNull(schemas.get(referencedMetadata.schema()));
        SQLTable referencedTable = Objects.requireNonNull(referencedSchema.getTable(referencedMetadata.table()));

        ForeignKey foreignKey = new ForeignKey(schema.getName(), table.getName(), referencedSchema.getName(), referencedTable.getName(), OnDelete.SET_NULL, OnUpdate.CASCADE);
        try {
            for (ForeignKey.Link link : parseLinks(oneToOne.link())) {
                foreignKey.addLink(link);
            }
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Error parsing @OneToOne link on field " + field.getName() + " in class " + clazz.getName() + ": " + e.getMessage(), e);
        } //todo: all columns in the link must be unique in our table, and must be id columns in the referenced table. this will be enforced by H2 but check here for better errors
        table.addForeignKey(foreignKey);


        Delete delete = field.getAnnotation(Delete.class);
        DeleteStrategy deleteStrategy = delete != null ? delete.value() : DeleteStrategy.NO_ACTION;
        table.addTrigger(new SQLDeleteStrategyTrigger(dataSchema, dataTable, referencedSchema.getName(), referencedTable.getName(), deleteStrategy, foreignKey.getLinkingColumns()));
    }

    private void parseLinks(ForeignKey foreignKey, String links) {
        List<ForeignKey.Link> parsedLinks = parseLinks(links);
        for (ForeignKey.Link link : parsedLinks) {
            foreignKey.addLink(link);
        }
    }

    private List<ForeignKey.Link> parseLinks(String links) {
        List<ForeignKey.Link> mappings = new ArrayList<>();
        for (String link : StringUtils.parseCommaSeperatedList(links)) {
            String[] parts = link.split("=");
            Preconditions.checkArgument(parts.length == 2, "Invalid link format! Expected format: localColumn=foreignColumn, got: " + link);
            mappings.add(new ForeignKey.Link(ValueUtils.parseValue(parts[1].trim()), ValueUtils.parseValue(parts[0].trim())));
        }
        return mappings;
    }
}
