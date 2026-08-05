package net.staticstudios.data.misc;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.parse.ForeignKey;
import net.staticstudios.data.parse.SQLColumn;
import net.staticstudios.data.parse.SQLSchema;
import net.staticstudios.data.parse.SQLTable;
import net.staticstudios.data.util.ColumnMetadata;
import net.staticstudios.data.util.SQLUtils;
import net.staticstudios.data.utils.Link;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public final class SchemaAssertions {
    private static final Pattern CREATE_TRIGGER_NAME = Pattern.compile(
            "CREATE\\s+TRIGGER(?:\\s+IF\\s+NOT\\s+EXISTS)?\\s+(?:\"([^\"]+)\"|([^\\s;]+))",
            Pattern.CASE_INSENSITIVE
    );

    private SchemaAssertions() {
    }

    public static void assertMatches(DataManager dataManager, Connection connection, DatabaseEngine engine, String... schemaNames) throws SQLException {
        for (String schemaName : schemaNames) {
            SQLSchema expectedSchema = dataManager.getSQLBuilder().getSchema(schemaName);
            assertNotNull(expectedSchema, "SQLBuilder did not contain expected schema " + schemaName);
            assertSchemaMatches(connection, engine, expectedSchema);
        }
    }

    private static void assertSchemaMatches(Connection connection, DatabaseEngine engine, SQLSchema expectedSchema) throws SQLException {
        String schemaName = expectedSchema.getName();
        Map<String, SQLTable> expectedTables = new HashMap<>();
        for (SQLTable table : expectedSchema.getTables()) {
            expectedTables.put(table.getName(), table);
        }

        Set<String> actualTables = readTableNames(connection, schemaName);
        assertEquals(expectedTables.keySet(), actualTables, engine + " tables in schema " + schemaName);

        for (SQLTable expectedTable : expectedTables.values()) {
            assertTableMatches(connection, engine, expectedTable);
        }
    }

    private static void assertTableMatches(Connection connection, DatabaseEngine engine, SQLTable expectedTable) throws SQLException {
        String schemaName = expectedTable.getSchema().getName();
        String tableName = expectedTable.getName();
        String context = engine + " table " + schemaName + "." + tableName;

        Map<String, ColumnShape> expectedColumns = new HashMap<>();
        Set<String> expectedIndexes = new HashSet<>();
        Set<String> expectedUniqueColumns = new HashSet<>();
        for (SQLColumn column : expectedTable.getColumns()) {
            if (engine == DatabaseEngine.POSTGRESQL && column.isVirtual()) {
                continue;
            }
            String sqlType = engine == DatabaseEngine.H2
                    ? SQLUtils.getH2SqlType(column.getType())
                    : SQLUtils.getPgSqlType(column.getType());
            expectedColumns.put(column.getName(), new ColumnShape(
                    normalizeType(sqlType),
                    column.isNullable(),
                    normalizeDefault(column.getDefaultValue())
            ));
            if (column.isIndexed() && !column.isUnique()) {
                expectedIndexes.add(("idx_" + schemaName + "_" + tableName + "_" + column.getName()).toLowerCase(Locale.ROOT));
            }
            if (column.isUnique() && expectedTable.getIdColumns().stream().noneMatch(id -> id.name().equals(column.getName()))) {
                expectedUniqueColumns.add(column.getName());
            }
        }

        DatabaseMetaData metadata = connection.getMetaData();
        Map<String, ColumnShape> actualColumns = readColumns(metadata, schemaName, tableName);
        assertEquals(expectedColumns, actualColumns, context + " columns");

        List<String> expectedPrimaryKey = expectedTable.getIdColumns().stream().map(ColumnMetadata::name).toList();
        assertEquals(expectedPrimaryKey, readPrimaryKey(metadata, schemaName, tableName), context + " primary key");

        Map<String, Set<ForeignKeyShape>> expectedForeignKeysByLegacyName = new HashMap<>();
        for (ForeignKey foreignKey : expectedTable.getForeignKeys()) {
            ForeignKeyShape shape = new ForeignKeyShape(
                    foreignKey.getLinkingColumns().stream().map(Link::columnInReferringTable).toList(),
                    foreignKey.getReferencedSchema(),
                    foreignKey.getReferencedTable(),
                    foreignKey.getLinkingColumns().stream().map(Link::columnInReferencedTable).toList(),
                    foreignKey.getOnUpdate() == null ? null : foreignKey.getOnUpdate().toString(),
                    foreignKey.getOnDelete() == null ? null : foreignKey.getOnDelete().toString()
            );
            String effectiveName = effectiveIdentifier(foreignKey.getName(), engine);
            expectedForeignKeysByLegacyName.computeIfAbsent(effectiveName, ignored -> new HashSet<>()).add(shape);
        }
        assertLegacyNamedForeignKeysMatch(
                expectedForeignKeysByLegacyName,
                readForeignKeys(metadata, schemaName, tableName),
                context
        );

        List<IndexShape> actualIndexes = readIndexes(metadata, schemaName, tableName);
        Set<String> actualNamedIndexes = new HashSet<>();
        for (IndexShape index : actualIndexes) {
            if (index.name().toLowerCase(Locale.ROOT).startsWith("idx_")) {
                actualNamedIndexes.add(index.name().toLowerCase(Locale.ROOT));
            }
        }
        assertEquals(expectedIndexes, actualNamedIndexes, context + " generated indexes");
        for (String uniqueColumn : expectedUniqueColumns) {
            assertTrue(actualIndexes.stream().anyMatch(index -> index.unique() && index.columns().equals(List.of(uniqueColumn))),
                    context + " should have a single-column unique constraint for " + uniqueColumn);
        }

        Set<String> expectedTriggerNames = new HashSet<>();
        expectedTable.getTriggers().stream()
                .map(trigger -> engine == DatabaseEngine.H2 ? trigger.getH2SQL() : trigger.getPgSQL())
                .map(SchemaAssertions::createdTriggerName)
                .filter(java.util.Objects::nonNull)
                .map(name -> effectiveIdentifier(name, engine))
                .forEach(expectedTriggerNames::add);
        long expectedTriggerCount = expectedTriggerNames.size();
        assertEquals(expectedTriggerCount, readStaticDataTriggerCount(connection, engine, schemaName, tableName), context + " Static Data triggers");
    }

    private static void assertLegacyNamedForeignKeysMatch(
            Map<String, Set<ForeignKeyShape>> expectedByName,
            Set<ForeignKeyShape> actual,
            String context
    ) {
        // Static Data has historically used unqualified foreign-key names. When two relationships on the same
        // table share that name, IF NOT EXISTS retains one of them. Preserve and test that behavior so upgrading
        // does not rename production constraints.
        assertEquals(expectedByName.size(), actual.size(), context + " foreign-key count after legacy-name collisions");
        Set<ForeignKeyShape> unmatchedActual = new HashSet<>(actual);
        for (Map.Entry<String, Set<ForeignKeyShape>> entry : expectedByName.entrySet()) {
            Set<ForeignKeyShape> matches = new HashSet<>(unmatchedActual);
            matches.retainAll(entry.getValue());
            assertEquals(1, matches.size(), context + " foreign key named " + entry.getKey());
            unmatchedActual.remove(matches.iterator().next());
        }
        assertTrue(unmatchedActual.isEmpty(), context + " had unexpected foreign keys " + unmatchedActual);
    }

    private static String createdTriggerName(String sql) {
        Matcher matcher = CREATE_TRIGGER_NAME.matcher(sql);
        if (!matcher.find()) {
            return null;
        }
        return matcher.group(1) == null ? matcher.group(2) : matcher.group(1);
    }

    private static String effectiveIdentifier(String identifier, DatabaseEngine engine) {
        String effective = identifier;
        if (engine == DatabaseEngine.POSTGRESQL && effective.length() > 63) {
            effective = effective.substring(0, 63);
        }
        return effective.toLowerCase(Locale.ROOT);
    }

    private static Set<String> readTableNames(Connection connection, String schemaName) throws SQLException {
        Set<String> tables = new HashSet<>();
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_type = 'BASE TABLE'"
        )) {
            statement.setString(1, schemaName);
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    tables.add(resultSet.getString(1));
                }
            }
        }
        return tables;
    }

    private static Map<String, ColumnShape> readColumns(DatabaseMetaData metadata, String schemaName, String tableName) throws SQLException {
        Map<String, ColumnShape> columns = new HashMap<>();
        try (ResultSet resultSet = metadata.getColumns(null, schemaName, tableName, null)) {
            while (resultSet.next()) {
                columns.put(resultSet.getString("COLUMN_NAME"), new ColumnShape(
                        normalizeType(resultSet.getString("TYPE_NAME")),
                        resultSet.getInt("NULLABLE") != DatabaseMetaData.columnNoNulls,
                        normalizeDefault(resultSet.getString("COLUMN_DEF"))
                ));
            }
        }
        return columns;
    }

    private static List<String> readPrimaryKey(DatabaseMetaData metadata, String schemaName, String tableName) throws SQLException {
        Map<Short, String> columns = new HashMap<>();
        try (ResultSet resultSet = metadata.getPrimaryKeys(null, schemaName, tableName)) {
            while (resultSet.next()) {
                columns.put(resultSet.getShort("KEY_SEQ"), resultSet.getString("COLUMN_NAME"));
            }
        }
        return columns.entrySet().stream().sorted(Map.Entry.comparingByKey()).map(Map.Entry::getValue).toList();
    }

    private static Set<ForeignKeyShape> readForeignKeys(DatabaseMetaData metadata, String schemaName, String tableName) throws SQLException {
        Map<String, ForeignKeyRows> keys = new LinkedHashMap<>();
        try (ResultSet resultSet = metadata.getImportedKeys(null, schemaName, tableName)) {
            while (resultSet.next()) {
                String keyName = resultSet.getString("FK_NAME");
                ForeignKeyRows rows = keys.computeIfAbsent(keyName, ignored -> new ForeignKeyRows(
                        resultSetValue(resultSet, "PKTABLE_SCHEM"),
                        resultSetValue(resultSet, "PKTABLE_NAME"),
                        ruleName(resultSetValueShort(resultSet, "UPDATE_RULE")),
                        ruleName(resultSetValueShort(resultSet, "DELETE_RULE"))
                ));
                short sequence = resultSet.getShort("KEY_SEQ");
                rows.localColumns.put(sequence, resultSet.getString("FKCOLUMN_NAME"));
                rows.referencedColumns.put(sequence, resultSet.getString("PKCOLUMN_NAME"));
            }
        }

        Set<ForeignKeyShape> foreignKeys = new LinkedHashSet<>();
        for (ForeignKeyRows rows : keys.values()) {
            foreignKeys.add(new ForeignKeyShape(
                    orderedValues(rows.localColumns),
                    rows.referencedSchema,
                    rows.referencedTable,
                    orderedValues(rows.referencedColumns),
                    rows.onUpdate,
                    rows.onDelete
            ));
        }
        return foreignKeys;
    }

    private static List<IndexShape> readIndexes(DatabaseMetaData metadata, String schemaName, String tableName) throws SQLException {
        Map<String, IndexRows> indexes = new LinkedHashMap<>();
        try (ResultSet resultSet = metadata.getIndexInfo(null, schemaName, tableName, false, false)) {
            while (resultSet.next()) {
                String indexName = resultSet.getString("INDEX_NAME");
                String columnName = resultSet.getString("COLUMN_NAME");
                if (indexName == null || columnName == null || resultSet.getShort("TYPE") == DatabaseMetaData.tableIndexStatistic) {
                    continue;
                }
                IndexRows rows = indexes.computeIfAbsent(indexName, ignored -> new IndexRows(!resultSetValueBoolean(resultSet, "NON_UNIQUE")));
                rows.columns.put(resultSet.getShort("ORDINAL_POSITION"), columnName);
            }
        }
        List<IndexShape> result = new ArrayList<>();
        for (Map.Entry<String, IndexRows> entry : indexes.entrySet()) {
            result.add(new IndexShape(entry.getKey(), entry.getValue().unique, orderedValues(entry.getValue().columns)));
        }
        return result;
    }

    private static long readStaticDataTriggerCount(Connection connection, DatabaseEngine engine, String schemaName, String tableName) throws SQLException {
        String sql = engine == DatabaseEngine.POSTGRESQL
                ? "SELECT COUNT(*) FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid JOIN pg_namespace n ON n.oid = c.relnamespace "
                + "WHERE n.nspname = ? AND c.relname = ? AND NOT t.tgisinternal AND t.tgname LIKE 'static_data_v3_%'"
                : "SELECT COUNT(*) FROM information_schema.triggers WHERE event_object_schema = ? AND event_object_table = ? AND trigger_name LIKE 'static_data_v3_%'";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, schemaName);
            statement.setString(2, tableName);
            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                return resultSet.getLong(1);
            }
        }
    }

    private static String normalizeType(String type) {
        String normalized = type.toUpperCase(Locale.ROOT).replaceAll("\\s+", " ").trim();
        return switch (normalized) {
            case "CHARACTER VARYING", "VARCHAR", "TEXT" -> "TEXT";
            case "INT4", "INTEGER" -> "INTEGER";
            case "INT8", "BIGINT" -> "BIGINT";
            case "FLOAT4", "REAL" -> "REAL";
            case "FLOAT8", "DOUBLE", "DOUBLE PRECISION" -> "DOUBLE PRECISION";
            case "BOOL", "BOOLEAN" -> "BOOLEAN";
            case "TIMESTAMPTZ", "TIMESTAMP WITH TIME ZONE" -> "TIMESTAMP WITH TIME ZONE";
            default -> normalized;
        };
    }

    private static String normalizeDefault(String defaultValue) {
        if (defaultValue == null) {
            return null;
        }
        String normalized = defaultValue
                .replaceAll("::(?:text|character varying|boolean|integer|bigint|real|double precision)$", "")
                .replaceAll("^\\((.*)\\)$", "$1")
                .trim();
        if (normalized.equalsIgnoreCase("true") || normalized.equalsIgnoreCase("false")) {
            return normalized.toUpperCase(Locale.ROOT);
        }
        return normalized;
    }

    private static String ruleName(short rule) {
        return switch (rule) {
            case DatabaseMetaData.importedKeyCascade -> "CASCADE";
            case DatabaseMetaData.importedKeySetNull -> "SET NULL";
            case DatabaseMetaData.importedKeySetDefault -> "SET DEFAULT";
            case DatabaseMetaData.importedKeyNoAction, DatabaseMetaData.importedKeyRestrict -> "NO ACTION";
            default -> throw new IllegalArgumentException("Unknown JDBC foreign-key rule " + rule);
        };
    }

    private static <T> List<T> orderedValues(Map<Short, T> values) {
        return values.entrySet().stream().sorted(Map.Entry.comparingByKey()).map(Map.Entry::getValue).toList();
    }

    private static String resultSetValue(ResultSet resultSet, String column) {
        try {
            return resultSet.getString(column);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static short resultSetValueShort(ResultSet resultSet, String column) {
        try {
            return resultSet.getShort(column);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static boolean resultSetValueBoolean(ResultSet resultSet, String column) {
        try {
            return resultSet.getBoolean(column);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public enum DatabaseEngine {
        H2,
        POSTGRESQL
    }

    private record ColumnShape(String type, boolean nullable, String defaultValue) {
    }

    private record ForeignKeyShape(
            List<String> localColumns,
            String referencedSchema,
            String referencedTable,
            List<String> referencedColumns,
            String onUpdate,
            String onDelete
    ) {
    }

    private record IndexShape(String name, boolean unique, List<String> columns) {
    }

    private static final class ForeignKeyRows {
        private final String referencedSchema;
        private final String referencedTable;
        private final String onUpdate;
        private final String onDelete;
        private final Map<Short, String> localColumns = new HashMap<>();
        private final Map<Short, String> referencedColumns = new HashMap<>();

        private ForeignKeyRows(String referencedSchema, String referencedTable, String onUpdate, String onDelete) {
            this.referencedSchema = referencedSchema;
            this.referencedTable = referencedTable;
            this.onUpdate = onUpdate;
            this.onDelete = onDelete;
        }
    }

    private static final class IndexRows {
        private final boolean unique;
        private final Map<Short, String> columns = new HashMap<>();

        private IndexRows(boolean unique) {
            this.unique = unique;
        }
    }
}
