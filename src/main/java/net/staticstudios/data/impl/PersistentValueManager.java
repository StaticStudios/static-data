package net.staticstudios.data.impl;

import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import net.staticstudios.data.DataDoesNotExistException;
import net.staticstudios.data.DataManager;
import net.staticstudios.data.data.InitialPersistentValue;
import net.staticstudios.data.data.PersistentValue;
import net.staticstudios.data.data.UniqueData;
import net.staticstudios.data.impl.pg.PostgresListener;
import net.staticstudios.data.impl.pg.PostgresOperation;
import net.staticstudios.data.key.CellKey;
import net.staticstudios.data.key.DataKey;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.*;

public class PersistentValueManager {
    private static PersistentValueManager instance;
    private final DataManager dataManager;
    private final PostgresListener pgListener;

    @SuppressWarnings("rawtypes")
    public PersistentValueManager(DataManager dataManager, PostgresListener pgListener) {
        this.dataManager = dataManager;
        this.pgListener = pgListener;

        pgListener.addHandler(notification -> {
            String schema = notification.getSchema();
            String table = notification.getTable();
            List<PersistentValue> dummyPersistentValues = dataManager.getDummyValues(schema + "." + table).stream()
                    .filter(value -> value.getClass() == PersistentValue.class)
                    .map(value -> (PersistentValue) value)
                    .toList();

            switch (notification.getOperation()) {
                case PostgresOperation.UPDATE, PostgresOperation.INSERT -> {
                    Map<String, String> newDataValueMap = notification.getData().newDataValueMap();
                    for (Map.Entry<String, String> entry : newDataValueMap.entrySet()) {
                        String column = entry.getKey();
                        PersistentValue<?> dummyPV = dummyPersistentValues.stream()
                                .filter(pv -> pv.getColumn().equals(column))
                                .findFirst()
                                .orElse(null);

                        if (dummyPV == null) {
                            continue;
                        }

                        String idColumn = dummyPV.getIdColumn();
                        UUID id = UUID.fromString(newDataValueMap.get(idColumn));

                        CellKey key = new CellKey(schema, table, column, id, idColumn);

                        String encodedValue = newDataValueMap.get(column); //Raw value as string
                        Object rawValue = dataManager.decode(dummyPV.getDataType(), encodedValue);
                        Object deserialized = dataManager.deserialize(dummyPV.getDataType(), rawValue);

                        dataManager.cache(key, deserialized, notification.getInstant());
                    }
                }
                case PostgresOperation.DELETE -> {
                    Map<String, String> oldDataValueMap = notification.getData().oldDataValueMap();
                    for (Map.Entry<String, String> entry : oldDataValueMap.entrySet()) {
                        String column = entry.getKey();
                        PersistentValue<?> dummyPV = dummyPersistentValues.stream()
                                .filter(pv -> pv.getColumn().equals(column))
                                .findFirst()
                                .orElse(null);

                        if (dummyPV == null) {
                            continue;
                        }

                        String idColumn = dummyPV.getIdColumn();
                        UUID id = UUID.fromString(oldDataValueMap.get(idColumn));

                        CellKey key = new CellKey(schema, table, column, id, idColumn);

                        dataManager.uncache(key);
                    }
                }
            }
        });
    }

    public static PersistentValueManager getInstance() {
        return instance;
    }

    public static void instantiate(DataManager dataManager, PostgresListener pgListener) {
        instance = new PersistentValueManager(dataManager, pgListener);
    }

    public void uncache(PersistentValue<?> persistentValue) {
        uncache(persistentValue.getSchema(),
                persistentValue.getTable(),
                persistentValue.getColumn(),
                persistentValue.getHolder().getRootHolder().getId(),
                persistentValue.getIdColumn()
        );
    }

    public void uncache(String schema, String table, String column, UUID holderId, String idColumn) {
        Object oldValue = null;

        DataKey key = new CellKey(schema, table, column, holderId, idColumn);

        try {
            oldValue = dataManager.get(key);
        } catch (DataDoesNotExistException ignored) {
        }
        dataManager.uncache(key);
        PersistentCollectionManager.getInstance().handlePersistentValueUncache(schema, table, column, holderId, idColumn, oldValue);
    }

    public void updateCache(PersistentValue<?> persistentValue, Object value) {
        updateCache(persistentValue.getSchema(),
                persistentValue.getTable(),
                persistentValue.getColumn(),
                persistentValue.getHolder().getRootHolder().getId(),
                persistentValue.getIdColumn(),
                value
        );
    }

    public void updateCache(String schema, String table, String column, UUID holderId, String idColumn, Object value) {
        Object oldValue = null;

        CellKey key = new CellKey(schema, table, column, holderId, idColumn);

        try {
            oldValue = dataManager.get(key);
        } catch (DataDoesNotExistException ignored) {
        }

        dataManager.cache(key, value, Instant.now());
        PersistentCollectionManager.getInstance().handlePersistentValueCacheUpdated(schema, table, column, holderId, idColumn, oldValue, value);
    }

    public void setInDatabase(List<InitialPersistentValue> initialData) throws Exception {
        if (initialData.isEmpty()) {
            return;
        }

        try (Connection connection = dataManager.getConnection()) {
            boolean autoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);

            //Group by id.schema.table
            Multimap<String, InitialPersistentValue> initialDataMap = Multimaps.newListMultimap(new HashMap<>(), ArrayList::new);

            for (InitialPersistentValue initial : initialData) {
                PersistentValue<?> pv = initial.getValue();
                String schemaTable = pv.getIdColumn() + "." + pv.getSchema() + "." + pv.getTable();
                initialDataMap.put(schemaTable, initial);
            }

            for (String idSchemaTable : initialDataMap.keySet()) {
                String idColumn = idSchemaTable.split("\\.", 2)[0];
                String schemaTable = idSchemaTable.split("\\.", 2)[1];
                Collection<InitialPersistentValue> initialDataValues = initialDataMap.get(idSchemaTable);

                StringBuilder sqlBuilder = new StringBuilder("INSERT INTO ");
                sqlBuilder.append(schemaTable);
                sqlBuilder.append(" (");
                sqlBuilder.append(idColumn);
                sqlBuilder.append(", ");
                for (InitialPersistentValue initial : initialDataValues) {
                    sqlBuilder.append(initial.getValue().getColumn());
                    sqlBuilder.append(", ");
                }
                sqlBuilder.setLength(sqlBuilder.length() - 2);

                sqlBuilder.append(") VALUES (?, ");
                sqlBuilder.append("?, ".repeat(initialDataValues.size()));
                sqlBuilder.setLength(sqlBuilder.length() - 2);
                sqlBuilder.append(") ON CONFLICT (");
                sqlBuilder.append(idColumn);
                sqlBuilder.append(") DO UPDATE SET ");
                for (InitialPersistentValue initial : initialDataValues) {
                    sqlBuilder.append(initial.getValue().getColumn());
                    sqlBuilder.append(" = EXCLUDED.");
                    sqlBuilder.append(initial.getValue().getColumn());
                    sqlBuilder.append(", ");
                }
                sqlBuilder.setLength(sqlBuilder.length() - 2);
                String sql = sqlBuilder.toString();

                dataManager.logSQL(sql);

                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    InitialPersistentValue first = initialDataValues.iterator().next();
                    statement.setObject(1, first.getValue().getHolder().getRootHolder().getId());
                    int i = 2;
                    for (InitialPersistentValue initial : initialDataValues) {
                        Object initialDataValue = initial.getInitialDataValue();
                        Object serialized = dataManager.serialize(initialDataValue);
                        statement.setObject(i++, serialized);
                    }

                    statement.executeUpdate();
                }
            }
            if (!autoCommit) {
                connection.commit();
            }
            connection.setAutoCommit(autoCommit);
        }
    }

    /**
     * Column keys are expected to all be in the same table, AND are expected to all have the same id column
     */
    public void loadAllFromDatabase(Connection connection, UniqueData dummyHolder, Collection<CellKey> dummyCellKeys) throws SQLException {
        if (dummyCellKeys.isEmpty()) {
            return;
        }

        CellKey firstCellKey = dummyCellKeys.iterator().next();
        String schemaTable = firstCellKey.getSchema() + "." + firstCellKey.getTable();
        pgListener.ensureTableHasTrigger(connection, schemaTable);

        String idColumn = firstCellKey.getIdColumn();

        Set<String> dataColumns = new HashSet<>();

        for (CellKey cellKey : dummyCellKeys) {
            dataColumns.add(cellKey.getColumn());
        }

        dataColumns.remove(idColumn);

        String sql;
        if (idColumn.equals(dummyHolder.getIdentifier().getColumn()) && schemaTable.equals(dummyHolder.getSchema() + "." + dummyHolder.getTable())) {
            StringBuilder sqlBuilder = new StringBuilder("SELECT ");
            for (String column : dataColumns) {
                sqlBuilder.append(column);
                sqlBuilder.append(", ");
            }
            sqlBuilder.append(idColumn);

            sqlBuilder.append(" FROM ");
            sqlBuilder.append(schemaTable);
            sql = sqlBuilder.toString();
        } else {
            StringBuilder sqlBuilder = new StringBuilder("SELECT ");
            for (String column : dataColumns) {
                sqlBuilder.append(column);
                sqlBuilder.append(", ");
            }
            sqlBuilder.append(dummyHolder.getSchema());
            sqlBuilder.append(".");
            sqlBuilder.append(dummyHolder.getTable());
            sqlBuilder.append(".");
            sqlBuilder.append(dummyHolder.getIdentifier().getColumn());

            sqlBuilder.append(" FROM ");
            sqlBuilder.append(schemaTable);

            sqlBuilder.append(" RIGHT JOIN ");
            sqlBuilder.append(dummyHolder.getSchema());
            sqlBuilder.append(".");
            sqlBuilder.append(dummyHolder.getTable());
            sqlBuilder.append(" ON ");
            sqlBuilder.append(schemaTable);
            sqlBuilder.append(".");
            sqlBuilder.append(idColumn);
            sqlBuilder.append(" = ");
            sqlBuilder.append(dummyHolder.getSchema());
            sqlBuilder.append(".");
            sqlBuilder.append(dummyHolder.getTable());
            sqlBuilder.append(".");
            sqlBuilder.append(dummyHolder.getIdentifier().getColumn());
            sql = sqlBuilder.toString();
        }

        dataManager.logSQL(sql);

        List<PersistentValue> dummyPersistentValues = dataManager.getDummyValues(schemaTable).stream()
                .filter(value -> value.getClass() == PersistentValue.class)
                .map(value -> (PersistentValue) value)
                .toList();

        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                UUID id = resultSet.getObject(dummyHolder.getIdentifier().getColumn(), UUID.class);
                for (String column : dataColumns) {
                    PersistentValue<?> dummyPV = dummyPersistentValues.stream()
                            .filter(pv -> pv.getColumn().equals(column))
                            .findFirst()
                            .orElse(null);

                    if (dummyPV == null) {
                        continue;
                    }

                    Object value = resultSet.getObject(column);
                    Object deserialized = dataManager.deserialize(dummyPV.getDataType(), value);
                    dataManager.cache(new CellKey(firstCellKey.getSchema(), firstCellKey.getTable(), column, id, idColumn), deserialized, Instant.now());
                }

                if (!dataColumns.contains(idColumn)) {
                    dataManager.cache(new CellKey(firstCellKey.getSchema(), firstCellKey.getTable(), idColumn, id, idColumn), id, Instant.now());
                }
            }
        }
    }
}
