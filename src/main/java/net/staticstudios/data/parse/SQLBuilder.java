package net.staticstudios.data.parse;

import com.google.common.base.Preconditions;
import net.staticstudios.data.*;
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

    public List<DDLStatement> parse(Class<? extends UniqueData> clazz) {
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

    private List<DDLStatement> getDefs(Collection<SQLSchema> schemas) { //todo: add indexes, uniques,
        List<DDLStatement> statements = new ArrayList<>();
        for (SQLSchema schema : schemas) {
            statements.add(DDLStatement.both("CREATE SCHEMA IF NOT EXISTS \"" + schema.getName() + "\";"));
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
                statements.add(DDLStatement.both(sb.toString()));
//                }
                if (!table.getColumns().isEmpty()) {
                    for (SQLColumn column : table.getColumns()) {
                        sb = new StringBuilder();
                        sb.append("ALTER TABLE \"").append(schema.getName()).append("\".\"").append(table.getName()).append("\" ").append("ADD COLUMN IF NOT EXISTS ").append("\"").append(column.getName()).append("\" ").append(SQLUtils.getSqlType(column.getType()));
                        if (!column.isNullable()) {
                            sb.append(" NOT NULL");
                        }
                        if (column.getDefaultValue() != null) {
                            sb.append(" DEFAULT ").append(column.getDefaultValue());
                        }

                        if (column.isIndexed()) {
//                            sb.append(" INDEXED");
                            //todo: this is not valid sql, need to create index separately
                        }

                        sb.append(";");
                        statements.add(DDLStatement.both(sb.toString()));
                    }
                }
            }
        }

        // define fkeys after table creation, to ensure all tables exist before adding fkeys
        for (SQLSchema schema : schemas) {
            for (SQLTable table : schema.getTables()) {
                for (ForeignKey foreignKey : table.getForeignKeys()) {
                    if (foreignKey == null) {
                        continue;
                    }
                    String fKeyName = "fk_" + table.getName() + "_" + String.join("_", foreignKey.getLinkingColumns().keySet()) + "_to_" + foreignKey.getSchema() + "_" + foreignKey.getTable() + "_" + String.join("_", foreignKey.getLinkingColumns().values());
                    StringBuilder sb = new StringBuilder();
                    sb.append("ALTER TABLE \"").append(schema.getName()).append("\".\"").append(table.getName()).append("\" ");
                    sb.append("ADD CONSTRAINT IF NOT EXISTS ").append(fKeyName).append(" ");
                    sb.append("FOREIGN KEY (");
                    for (String localCol : foreignKey.getLinkingColumns().keySet()) {
                        sb.append("\"").append(localCol).append("\", ");
                    }
                    sb.setLength(sb.length() - 2);
                    sb.append(") ");
                    sb.append("REFERENCES \"").append(foreignKey.getSchema()).append("\".\"").append(foreignKey.getTable()).append("\" (");
                    for (String foreignCol : foreignKey.getLinkingColumns().values()) {
                        sb.append("\"").append(foreignCol).append("\", ");
                    }
                    sb.setLength(sb.length() - 2);
                    sb.append(") ON DELETE CASCADE;"); //todo: on delete cascade or no action or set null? depends on type
                    String h2 = sb.toString();


                    sb = new StringBuilder();
                    sb.append("DO $$ BEGIN ");
                    sb.append("IF NOT EXISTS (SELECT 1 FROM information_schema.table_constraints WHERE constraint_name = '").append(fKeyName).append("' AND table_name = '").append(table.getName()).append("' AND constraint_schema = '").append(schema.getName()).append("' AND constraint_type = 'FOREIGN KEY') THEN ");

                    sb.append("ALTER TABLE \"").append(schema.getName()).append("\".\"").append(table.getName()).append("\" ");
                    sb.append("ADD CONSTRAINT ").append(fKeyName).append(" ");
                    sb.append("FOREIGN KEY (");
                    for (String localCol : foreignKey.getLinkingColumns().keySet()) {
                        sb.append("\"").append(localCol).append("\", ");
                    }
                    sb.setLength(sb.length() - 2);
                    sb.append(") ");
                    sb.append("REFERENCES \"").append(foreignKey.getSchema()).append("\".\"").append(foreignKey.getTable()).append("\" (");
                    for (String foreignCol : foreignKey.getLinkingColumns().values()) {
                        sb.append("\"").append(foreignCol).append("\", ");
                    }
                    sb.setLength(sb.length() - 2);
                    sb.append(") ON DELETE CASCADE;"); //todo: on delete cascade or no action or set null? depends on type
                    sb.append(" END IF; END $$;");
                    String pg = sb.toString();
                    statements.add(DDLStatement.of(h2, pg));
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
            //todo: when parsing a OneToOne relation, in the link if there is a name in our table that we have no already created, then we need to create it. note that the type should be the same as the referenced name type.
            //todo: add COLUMN REFERENCES bla bla bla for foreign keys

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
            String defaultValue;
            if (idColumn != null) {
                schemaName = ValueUtils.parseValue(dataAnnotation.schema());
                tableName = ValueUtils.parseValue(dataAnnotation.table());
                columnName = ValueUtils.parseValue(idColumn.name());
                nullable = false;
                indexed = false;
                defaultValue = "";
            } else if (columnAnnotation != null) {
                schemaName = ValueUtils.parseValue(columnAnnotation.schema());
                tableName = ValueUtils.parseValue(columnAnnotation.table());
                columnName = ValueUtils.parseValue(columnAnnotation.name());
                nullable = columnAnnotation.nullable();
                indexed = columnAnnotation.index();
                defaultValue = columnAnnotation.defaultValue();
            } else if (foreignColumn != null) {
                schemaName = ValueUtils.parseValue(foreignColumn.schema());
                tableName = ValueUtils.parseValue(foreignColumn.table());
                columnName = ValueUtils.parseValue(foreignColumn.name());
                nullable = foreignColumn.nullable();
                indexed = foreignColumn.index();
                defaultValue = foreignColumn.defaultValue();
            } else {
                continue;
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
                        Preconditions.checkNotNull(found, "Link name %s in OneToOne annotation on field %s in class %s is not an ID name", localColumn, field.getName(), clazz.getName());

                        idColumns.add(new ColumnMetadata(schemaName, tableName, otherColumn, found.type(), false, false, ""));
                    }
                }

                table = new SQLTable(schema, tableName, idColumns);
                schema.addTable(table);

                if (foreignColumn != null) {
                    for (ColumnMetadata idCol : table.getIdColumns()) {
                        Preconditions.checkState(table.getColumn(idCol.name()) == null, "ID column name " + idCol.name() + " in table " + tableName + " is duplicated!");
                        SQLColumn sqlColumn = new SQLColumn(table, idCol.type(), idCol.name(), false, false, null);
                        table.addColumn(sqlColumn);
                    }
                }
            }

            if (foreignColumn != null) {
                SQLSchema dataSqlSchema = schemas.computeIfAbsent(dataSchema, SQLSchema::new);
                SQLTable dataSqlTable = dataSqlSchema.getTable(dataTable);
                if (dataSqlTable == null) {
                    dataSqlTable = new SQLTable(dataSqlSchema, dataTable, metadata.idColumns());
                    dataSqlSchema.addTable(dataSqlTable);
                }

                String otherSchema = ValueUtils.parseValue(foreignColumn.schema());
                if (otherSchema.isEmpty()) {
                    otherSchema = schemaName;
                }
                String otherTable = ValueUtils.parseValue(foreignColumn.table());
                if (otherTable.isEmpty()) {
                    otherTable = tableName;
                }

                ForeignKey foreignKey = new ForeignKey(otherSchema, otherTable, foreignColumn.name());
                for (String link : StringUtils.parseCommaSeperatedList(foreignColumn.link())) {
                    String[] parts = link.split("=");
                    Preconditions.checkArgument(parts.length == 2, "Invalid link format in ForeignColumn annotation on field %s in class %s. Expected format: localColumn=foreignColumn", field.getName(), clazz.getName());

                    String localColumn = ValueUtils.parseValue(parts[0].trim());
                    String otherColumn = ValueUtils.parseValue(parts[1].trim());
                    foreignKey.addColumnMapping(localColumn, otherColumn);
                }

                dataSqlTable.getForeignKeys().add(foreignKey);
            }


            Class<?> type = ReflectionUtils.getGenericType(field); //todo: handle custom types to sql types
            SQLColumn sqlColumn = new SQLColumn(table, type, columnName, nullable, indexed, defaultValue.isEmpty() ? null : SQLUtils.parseDefaultValue(type, defaultValue));

            SQLColumn existingColumn = table.getColumn(columnName);
            if (existingColumn != null) {
                Preconditions.checkState(existingColumn.equals(sqlColumn), "Column " + columnName + " in table " + tableName + " has conflicting definitions! Existing: " + existingColumn + ", New: " + sqlColumn);
                continue;
            }

            table.addColumn(sqlColumn);
        }
    }
}
