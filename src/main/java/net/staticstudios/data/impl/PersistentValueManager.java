package net.staticstudios.data.impl;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.data.InitialPersistentValue;
import net.staticstudios.data.data.PersistentValue;
import net.staticstudios.data.data.UniqueData;
import net.staticstudios.data.impl.pg.PostgresListener;
import net.staticstudios.data.impl.pg.PostgresOperation;
import net.staticstudios.data.key.CellKey;

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

    public void updateCache(PersistentValue<?> persistentValue, Object value) {
        dataManager.cache(new CellKey(persistentValue), value, Instant.now());
    }

    public void setInDatabase(List<InitialPersistentValue> initialData) throws Exception {
        try (Connection connection = dataManager.getConnection()) {
            boolean autoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            for (InitialPersistentValue initial : initialData) {
                PersistentValue<?> data = initial.getValue();
                String schema = data.getSchema();
                String table = data.getTable();
                String column = data.getColumn();
                String idColumn = data.getIdColumn();

                String sql = "INSERT INTO " + schema + "." + table + " (" + column + ", " + idColumn + ") VALUES (?, ?) ON CONFLICT (" + idColumn + ") DO UPDATE SET " + column + " = EXCLUDED." + column;
                dataManager.logSQL(sql);

                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    Object serialized = dataManager.serialize(initial.getInitialDataValue());

                    statement.setObject(1, serialized);
                    statement.setObject(2, data.getHolder().getRootHolder().getId());


                    statement.executeUpdate();
                }
            }
            connection.commit();
            connection.setAutoCommit(autoCommit);
        }
    }

    /**
     * Column keys are expected to all be in the same table, AND are expected to all have the same id column
     *
     * @param connection
     * @param dummyHolder
     * @param dummyCellKeys
     * @return
     * @throws SQLException
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
