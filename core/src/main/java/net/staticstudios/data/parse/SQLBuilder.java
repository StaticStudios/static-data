package net.staticstudios.data.parse;

import com.google.common.base.Preconditions;
import net.staticstudios.data.*;
import net.staticstudios.data.impl.data.PersistentManyToManyCollectionImpl;
import net.staticstudios.data.util.*;
import net.staticstudios.data.utils.Link;
import net.staticstudios.data.utils.StringUtils;
import org.intellij.lang.annotations.Language;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.*;

/**
 * This class is responsible for parsing annotations and/or fields in all classes which extend {@link UniqueData}.
 * This class then converts that information into SQL metadata, which is later used to generate DDL statements for both Postgres and H2.
 */
public class SQLBuilder {
    public static final String INDENT = "  ";
    private static final Logger logger = LoggerFactory.getLogger(SQLBuilder.class);
    private final Map<String, SQLSchema> parsedSchemas;
    private final DataManager dataManager;

    public SQLBuilder(DataManager dataManager) {
        this.dataManager = dataManager;
        this.parsedSchemas = new HashMap<>();
    }

    public static void parseLinks(ForeignKey foreignKey, String links) {
        for (Link link : parseLinks(links)) {
            foreignKey.addLink(link);
        }
    }

    public static void parseLinksReversed(ForeignKey foreignKey, String links) {
        for (Link link : parseLinksReversed(links)) {
            foreignKey.addLink(link);
        }
    }

    public static List<Link> parseLinksReversed(String links) {
        List<Link> mappings = new ArrayList<>();
        for (Link rawLink : Link.parseRawLinksReversed(links)) {
            mappings.add(new Link(ValueUtils.parseValue(rawLink.columnInReferencedTable()), ValueUtils.parseValue(rawLink.columnInReferringTable())));
        }
        return mappings;
    }

    public static List<Link> parseLinks(String links) {
        List<Link> mappings = new ArrayList<>();
        for (Link rawLink : Link.parseRawLinks(links)) {
            mappings.add(new Link(ValueUtils.parseValue(rawLink.columnInReferencedTable()), ValueUtils.parseValue(rawLink.columnInReferringTable())));
        }
        return mappings;
    }

    public List<DDLStatement> parse(Class<? extends UniqueData> clazz) {
        Preconditions.checkNotNull(clazz, "Class cannot be null");

        Set<Class<? extends UniqueData>> visited = walk(clazz);
        Map<String, SQLSchema> schemas = new HashMap<>();

        // shouldn't matter the order of the new classes since we parse relations only after creating tables. so dependencies will always exist
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
                        Preconditions.checkState(existingColumn.equals(newColumn), "Column " + newColumn.getName() + " in referringTable " + newTable.getName() + " has conflicting definitions! Existing: " + existingColumn + ", New: " + newColumn);
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
                boolean skipPKDef = false;
                for (ColumnMetadata idColumn : table.getIdColumns()) {
                    if (idColumn instanceof AutoIncrementingIntegerColumnMetadata) {
                        Preconditions.checkArgument(table.getIdColumns().size() == 1, "Auto-incrementing ID column can only be used as the sole ID column in referringTable " + table.getName());
                        h2Sb.append(INDENT).append("\"").append(idColumn.name()).append("\" ").append("BIGINT AUTO_INCREMENT PRIMARY KEY\n");
                        pgSb.append(INDENT).append("\"").append(idColumn.name()).append("\" ").append("BIGSERIAL PRIMARY KEY\n");
                        skipPKDef = true;
                    } else {
                        h2Sb.append(INDENT).append("\"").append(idColumn.name()).append("\" ").append(SQLUtils.getH2SqlType(idColumn.type())).append(",\n");
                        pgSb.append(INDENT).append("\"").append(idColumn.name()).append("\" ").append(SQLUtils.getPgSqlType(idColumn.type())).append(" NOT NULL,\n");
                    }
                }
                if (!skipPKDef) {
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
                }
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

        // define fkeys after referringTable creation, to ensure all tables exist before adding fkeys
        for (SQLSchema schema : schemas) { //todo: what if an fkey's on delete/ on cascade strategy has changed?
            for (SQLTable table : schema.getTables()) {
                for (ForeignKey foreignKey : table.getForeignKeys()) {
                    if (foreignKey == null) {
                        continue;
                    }
                    String fKeyName = foreignKey.getName();
                    StringBuilder sb = new StringBuilder();
                    sb.append("ALTER TABLE \"").append(foreignKey.getReferringSchema()).append("\".\"").append(foreignKey.getReferringTable()).append("\" ");
                    sb.append("ADD CONSTRAINT IF NOT EXISTS ").append(fKeyName).append(" ");
                    sb.append("FOREIGN KEY (");
                    for (Link link : foreignKey.getLinkingColumns()) {
                        sb.append("\"").append(link.columnInReferringTable()).append("\", ");
                    }
                    sb.setLength(sb.length() - 2);
                    sb.append(") ");
                    sb.append("REFERENCES \"").append(foreignKey.getReferencedSchema()).append("\".\"").append(foreignKey.getReferencedTable()).append("\" (");
                    for (Link link : foreignKey.getLinkingColumns()) {
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
                    for (Link link : foreignKey.getLinkingColumns()) {
                        sb.append("\"").append(link.columnInReferringTable()).append("\", ");
                    }
                    sb.setLength(sb.length() - 2);
                    sb.append(") ");
                    sb.append("REFERENCES \"").append(foreignKey.getReferencedSchema()).append("\".\"").append(foreignKey.getReferencedTable()).append("\" (");
                    for (Link link : foreignKey.getLinkingColumns()) {
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
            Class<?> genericType = ReflectionUtils.getGenericType(field);
            if (genericType == null || !UniqueData.class.isAssignableFrom(genericType)) {
                continue;
            }
            Class<? extends UniqueData> related = genericType.asSubclass(UniqueData.class);
            walk(related, visited);
        }
    }

    private void parseIndividualColumns(Class<? extends UniqueData> clazz, Map<String, SQLSchema> schemas) {
        logger.trace("Parsing columnsInReferringTable for class {}", clazz.getName());
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
            parsePersistentCollection(clazz, schemas, dataAnnotation, metadata, field);
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
            schemaName = ValueUtils.parseValue(dataAnnotation.schema());
            tableName = ValueUtils.parseValue(dataAnnotation.table());
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
            Preconditions.checkArgument(!(schemaName.equals(dataSchema) && tableName.equals(dataTable)), "ForeignColumn field %s in class %s cannot reference its own referringTable", field.getName(), clazz.getName());
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
                    Preconditions.checkState(table.getColumn(idCol.name()) == null, "ID column name " + idCol.name() + " in referringTable " + tableName + " is duplicated!");
                    SQLColumn sqlColumn = new SQLColumn(table, idCol.type(), idCol.name(), false, false, true, null);
                    table.addColumn(sqlColumn);
                }
            }
        } else if (foreignColumn != null) {
            List<Link> links = parseLinks(foreignColumn.link());
            for (Link link : links) {
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

            Preconditions.checkArgument(!(referencedSchema.equals(dataSchema) && referencedTable.equals(dataTable)), "ForeignColumn field %s in class %s cannot reference its own referringTable", field.getName(), clazz.getName());

            ForeignKey foreignKey = new ForeignKey(dataSchema, dataTable, referencedSchema, referencedTable, OnDelete.CASCADE);
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
            Preconditions.checkState(existingColumn.equals(sqlColumn), "Column " + columnName + " in referringTable " + tableName + " has conflicting definitions! Existing: " + existingColumn + ", New: " + sqlColumn);
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
        Preconditions.checkNotNull(oneToOne, "Reference field " + field.getName() + " in class " + clazz.getName() + " is not annotated with @OneToOne");

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

        ForeignKey foreignKey = new ForeignKey(schema.getName(), table.getName(), referencedSchema.getName(), referencedTable.getName(), OnDelete.SET_NULL);
        try {
            parseLinks(foreignKey, oneToOne.link());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Error parsing @OneToOne link on field " + field.getName() + " in class " + clazz.getName() + ": " + e.getMessage(), e);
        }
        table.addForeignKey(foreignKey);

        Delete delete = field.getAnnotation(Delete.class);
        DeleteStrategy deleteStrategy = delete != null ? delete.value() : DeleteStrategy.NO_ACTION;
        table.addTrigger(new SQLDeleteStrategyTrigger(dataSchema, dataTable, referencedSchema.getName(), referencedTable.getName(), deleteStrategy, foreignKey.getLinkingColumns()));
    }

    private void parsePersistentCollection(Class<? extends UniqueData> clazz, Map<String, SQLSchema> schemas, Data dataAnnotation, UniqueDataMetadata metadata, Field field) {
        if (!field.getType().equals(PersistentCollection.class)) {
            return;
        }
        Class<?> genericType = ReflectionUtils.getGenericType(field);
        Preconditions.checkNotNull(genericType, "Field " + field.getName() + " in class " + clazz.getName() + " is not parameterized!");
        OneToMany oneToMany = field.getAnnotation(OneToMany.class);
        ManyToMany manyToMany = field.getAnnotation(ManyToMany.class);
        int annotationsCount = 0;
        if (oneToMany != null) annotationsCount++;
        if (manyToMany != null) annotationsCount++;
        Preconditions.checkArgument(annotationsCount == 1, "Field " + field.getName() + " in class " + clazz.getName() + " must be annotated with either @OneToMany or @ManyToMany");

        if (oneToMany != null) {
            if (UniqueData.class.isAssignableFrom(genericType)) {
                parseOneToManyPersistentCollection(oneToMany, genericType.asSubclass(UniqueData.class), clazz, schemas, dataAnnotation, metadata, field);
            } else {
                parseOneToManyValuePersistentCollection(oneToMany, genericType, clazz, schemas, dataAnnotation, metadata, field);
            }
        }
        if (manyToMany != null) {
            Preconditions.checkArgument(UniqueData.class.isAssignableFrom(genericType), "Field " + field.getName() + " in class " + clazz.getName() + " is not parameterized with a UniqueData type! Generic type: " + genericType);
            parseManyToManyPersistentCollection(manyToMany, genericType.asSubclass(UniqueData.class), clazz, schemas, dataAnnotation, metadata, field);
        }
    }

    private void parseOneToManyValuePersistentCollection(OneToMany oneToMany, Class<?> genericType, Class<? extends UniqueData> clazz, Map<String, SQLSchema> schemas, Data dataAnnotation, UniqueDataMetadata metadata, Field field) {
        String dataSchema = ValueUtils.parseValue(dataAnnotation.schema());
        String dataTable = ValueUtils.parseValue(dataAnnotation.table());

        String referencedSchemaName = oneToMany.schema();
        if (referencedSchemaName.isEmpty()) {
            referencedSchemaName = dataSchema;
        }
        referencedSchemaName = ValueUtils.parseValue(referencedSchemaName);
        String referencedTableName = ValueUtils.parseValue(oneToMany.table());
        String referencedColumnName = ValueUtils.parseValue(oneToMany.column());

        Preconditions.checkArgument(!referencedTableName.isEmpty(), "OneToMany PersistentCollection field " + field.getName() + " in class " + clazz.getName() + " must specify a table name in the @OneToMany annotation when the generic type is not a UniqueData type.");
        Preconditions.checkArgument(!referencedColumnName.isEmpty(), "OneToMany PersistentCollection field " + field.getName() + " in class " + clazz.getName() + " must specify a column name in the @OneToMany annotation when the generic type is not a UniqueData type.");

        SQLSchema schema = Objects.requireNonNull(schemas.get(dataSchema));
        SQLTable table = Objects.requireNonNull(schema.getTable(dataTable));

        SQLSchema referencedSchema = schemas.computeIfAbsent(referencedSchemaName, SQLSchema::new);
        SQLTable referencedTable = referencedSchema.getTable(referencedTableName);
        if (referencedTable == null) {
            List<ColumnMetadata> idColumns = List.of(new AutoIncrementingIntegerColumnMetadata(referencedSchemaName, referencedTableName, referencedTableName + "_id"));
            referencedTable = new SQLTable(referencedSchema, referencedTableName, idColumns);
            referencedSchema.addTable(referencedTable);
            referencedTable.addColumn(new SQLColumn(referencedTable, genericType, referencedColumnName, oneToMany.nullable(), oneToMany.indexed(), oneToMany.unique(), null));
            for (Link link : parseLinks(oneToMany.link())) {
                Class<?> columnType = null;
                SQLColumn columnInReferringTable = table.getColumn(link.columnInReferringTable());
                if (columnInReferringTable != null) {
                    columnType = columnInReferringTable.getType();
                }
                Preconditions.checkNotNull(columnType, "Link name %s in OneToMany annotation on field %s in class %s is not an ID name", link.columnInReferringTable(), field.getName(), clazz.getName());
                SQLColumn linkingColumn = new SQLColumn(referencedTable, columnType, link.columnInReferencedTable(), false, false, false, null);
                referencedTable.addColumn(linkingColumn);
            }

        } //if non-null don't attempt to create/structure it, assume the user knows what they're doing.

        Delete delete = field.getAnnotation(Delete.class);
        DeleteStrategy deleteStrategy = delete != null ? delete.value() : DeleteStrategy.NO_ACTION;
        OnDelete onDelete = deleteStrategy == DeleteStrategy.CASCADE ? OnDelete.CASCADE : OnDelete.SET_NULL;

        // unlike a Reference, this foreign key goes on the referenced referringTable, not our referringTable.
        // Since the foreign key is on the other referringTable, let the foreign key handle the deletion strategy instead of a trigger.
        ForeignKey foreignKey = new ForeignKey(referencedSchemaName, referencedTableName, schema.getName(), table.getName(), onDelete);
        try {
            parseLinksReversed(foreignKey, oneToMany.link());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Error parsing @OneToMany link on field " + field.getName() + " in class " + clazz.getName() + ": " + e.getMessage(), e);
        }
        referencedTable.addForeignKey(foreignKey);
    }

    private void parseOneToManyPersistentCollection(OneToMany oneToMany, Class<? extends UniqueData> genericType, Class<? extends UniqueData> clazz, Map<String, SQLSchema> schemas, Data dataAnnotation, UniqueDataMetadata metadata, Field field) {
        UniqueDataMetadata referencedMetadata = dataManager.getMetadata(genericType);
        Preconditions.checkNotNull(referencedMetadata, "No metadata found for referenced class " + genericType.getName());

        String dataSchema = ValueUtils.parseValue(dataAnnotation.schema());
        String dataTable = ValueUtils.parseValue(dataAnnotation.table());

        SQLSchema schema = Objects.requireNonNull(schemas.get(dataSchema));
        SQLTable table = Objects.requireNonNull(schema.getTable(dataTable));

        SQLSchema referencedSchema = Objects.requireNonNull(schemas.get(referencedMetadata.schema()));
        SQLTable referencedTable = Objects.requireNonNull(referencedSchema.getTable(referencedMetadata.table()));

        Delete delete = field.getAnnotation(Delete.class);
        DeleteStrategy deleteStrategy = delete != null ? delete.value() : DeleteStrategy.NO_ACTION;
        OnDelete onDelete = deleteStrategy == DeleteStrategy.CASCADE ? OnDelete.CASCADE : OnDelete.SET_NULL;

        // unlike a Reference, this foreign key goes on the referenced referringTable, not our referringTable.
        // Since the foreign key is on the other referringTable, let the foreign key handle the deletion strategy instead of a trigger.
        ForeignKey foreignKey = new ForeignKey(referencedSchema.getName(), referencedTable.getName(), schema.getName(), table.getName(), onDelete);
        try {
            parseLinksReversed(foreignKey, oneToMany.link());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Error parsing @OneToMany link on field " + field.getName() + " in class " + clazz.getName() + ": " + e.getMessage(), e);
        }
        referencedTable.addForeignKey(foreignKey);
    }

    private void parseManyToManyPersistentCollection(ManyToMany manyToMany, Class<? extends UniqueData> genericType, Class<? extends UniqueData> clazz, Map<String, SQLSchema> schemas, Data dataAnnotation, UniqueDataMetadata metadata, Field field) {
        UniqueDataMetadata referencedMetadata = dataManager.getMetadata(genericType);
        Preconditions.checkNotNull(referencedMetadata, "No metadata found for referenced class " + genericType.getName());

        String dataSchema = ValueUtils.parseValue(dataAnnotation.schema());
        String dataTable = ValueUtils.parseValue(dataAnnotation.table());

        SQLSchema schema = Objects.requireNonNull(schemas.get(dataSchema));
        SQLTable table = Objects.requireNonNull(schema.getTable(dataTable));

        SQLSchema referencedSchema = Objects.requireNonNull(schemas.get(referencedMetadata.schema()));
        SQLTable referencedTable = Objects.requireNonNull(referencedSchema.getTable(referencedMetadata.table()));

        String joinTableSchemaName = PersistentManyToManyCollectionImpl.getJoinTableSchema(ValueUtils.parseValue(manyToMany.joinTableSchema()), dataSchema);
        String joinTableName = PersistentManyToManyCollectionImpl.getJoinTableName(ValueUtils.parseValue(manyToMany.joinTable()), dataTable, referencedMetadata.table());

        List<Link> joinTableToDataTableLinks;
        List<Link> joinTableToReferencedTableLinks;

        try {
            joinTableToDataTableLinks = PersistentManyToManyCollectionImpl.getJoinTableToDataTableLinks(dataTable, manyToMany.link());
            joinTableToReferencedTableLinks = PersistentManyToManyCollectionImpl.getJoinTableToReferencedTableLinks(dataTable, referencedTable.getName(), manyToMany.link());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Error parsing @ManyToMany link on field " + field.getName() + " in class " + clazz.getName() + ": " + e.getMessage(), e);
        }

        String referencedTableColumnPrefix = PersistentManyToManyCollectionImpl.getReferencedTableColumnPrefix(dataTable, referencedTable.getName());
        String dataTableColumnPrefix = PersistentManyToManyCollectionImpl.getDataTableColumnPrefix(dataTable);

        SQLSchema joinSchema = schemas.computeIfAbsent(joinTableSchemaName, SQLSchema::new);
        SQLTable joinTable = joinSchema.getTable(joinTableName);
        if (joinTable == null) {
            List<ColumnMetadata> joinTableIdColumns = new ArrayList<>();
            for (Link dataLink : joinTableToDataTableLinks) {
                SQLColumn foundColumn = null;
                for (SQLColumn column : table.getColumns()) {
                    if (column.getName().equals(dataLink.columnInReferencedTable())) {
                        foundColumn = column;
                        break;
                    }
                }
                Preconditions.checkNotNull(foundColumn, "Column not found in data referringTable! " + dataLink.columnInReferringTable());
                joinTableIdColumns.add(new ColumnMetadata(joinTableSchemaName, joinTableName, dataTableColumnPrefix + "_" + foundColumn.getName(), foundColumn.getType(), false, false, ""));
            }
            for (Link referencedLink : joinTableToReferencedTableLinks) {
                SQLColumn foundColumn = null;
                for (SQLColumn column : referencedTable.getColumns()) {
                    if (column.getName().equals(referencedLink.columnInReferencedTable())) {
                        foundColumn = column;
                        break;
                    }
                }
                Preconditions.checkNotNull(foundColumn, "Column not found in referenced referringTable! " + referencedLink.columnInReferringTable());
                joinTableIdColumns.add(new ColumnMetadata(joinTableSchemaName, joinTableName, referencedTableColumnPrefix + "_" + foundColumn.getName(), foundColumn.getType(), false, false, ""));
            }
            joinTable = new SQLTable(joinSchema, joinTableName, joinTableIdColumns);
            joinSchema.addTable(joinTable);
        }

        Delete delete = field.getAnnotation(Delete.class);
        DeleteStrategy deleteStrategy = delete != null ? delete.value() : DeleteStrategy.NO_ACTION;
        OnDelete onDelete = OnDelete.CASCADE;
        //todo: deletion strategy in this case is different than in the one to many case.
        // it should always cascade on the join referringTable, but depending on the delete strategy, we may or may not delete the referenced data.
        // impl with a trigger?

        ForeignKey foreignKeyJoinToDataTable = new ForeignKey(joinSchema.getName(), joinTable.getName(), schema.getName(), table.getName(), onDelete);
        ForeignKey foreignKeyJoinToReferenceTable = new ForeignKey(joinSchema.getName(), joinTable.getName(), referencedSchema.getName(), referencedTable.getName(), onDelete);
        joinTableToDataTableLinks.forEach(foreignKeyJoinToDataTable::addLink);
        joinTableToReferencedTableLinks.forEach(foreignKeyJoinToReferenceTable::addLink);
        joinTable.addForeignKey(foreignKeyJoinToDataTable);
        joinTable.addForeignKey(foreignKeyJoinToReferenceTable);
    }
}
