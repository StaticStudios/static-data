package net.staticstudios.data.impl;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.data.InitialPersistentValue;
import net.staticstudios.data.data.PersistentValue;
import net.staticstudios.data.data.UniqueData;
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

    public PersistentValueManager(DataManager dataManager, PostgresListener pgListener) {
        this.dataManager = dataManager;
        this.pgListener = pgListener;

        pgListener.addHandler(notification -> {
            //todo: something
            switch (notification.getOperation()) {
                case PostgresOperation.INSERT -> {
                    //todo: create the new unique data objects (there might be multiple)
                    updateValueCache(notification.getSchema(), notification.getTable(), notification.getDataValueMap(), notification.getInstant());
                }
                case PostgresOperation.UPDATE -> {
                    updateValueCache(notification.getSchema(), notification.getTable(), notification.getDataValueMap(), notification.getInstant());
                }
                case PostgresOperation.DELETE -> {
                    //todo: remove the unique data objects (there might be multiple)
                    //todo: remove values from the cache
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

    @SuppressWarnings("rawtypes")
    private void updateValueCache(String schema, String table, Map<String, String> dataValueMap, Instant instant) {
        List<PersistentValue> dummyPersistentValues = dataManager.getDummyValues(schema, table).stream()
                .filter(value -> value.getClass() == PersistentValue.class)
                .map(value -> (PersistentValue) value)
                .toList();

        for (Map.Entry<String, String> entry : dataValueMap.entrySet()) {
            String column = entry.getKey();
            PersistentValue<?> dummyPV = dummyPersistentValues.stream()
                    .filter(pv -> pv.getColumn().equals(column))
                    .findFirst()
                    .orElse(null);

            if (dummyPV == null) {
                continue;
            }

            String idColumn = dummyPV.getIdColumn();
            UUID id = UUID.fromString(dataValueMap.get(idColumn));

            CellKey key = new CellKey(schema, table, column, id, idColumn);

            String encodedValue = dataValueMap.get(column); //Raw value as string
            Object rawValue = dataManager.decode(dummyPV.getDataType(), encodedValue);
            Object deserialized = dataManager.deserialize(dummyPV.getDataType(), rawValue);

            dataManager.cache(key, deserialized, instant);
        }
    }

    public void updateCache(PersistentValue<?> persistentValue, Object value) {
        dataManager.cache(new CellKey(persistentValue), value, Instant.now());
    }

    public void setInDataSource(List<InitialPersistentValue> initialData) throws Exception {
        try (Connection connection = dataManager.getConnection()) {
            boolean autoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            for (InitialPersistentValue initial : initialData) {
                PersistentValue<?> data = initial.getValue();
                pgListener.ensureTableHasTrigger(connection, data.getTable());
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
     * @param cellKeys
     * @return
     * @throws SQLException
     */
    public void loadAllFromDataSource(Connection connection, UniqueData dummyHolder, Collection<CellKey> cellKeys) throws SQLException {
        if (cellKeys.isEmpty()) {
            return;
        }

        CellKey firstCellKey = cellKeys.iterator().next();
        String idColumn = firstCellKey.getIdColumn();
        String schemaTable = firstCellKey.getSchema() + "." + firstCellKey.getTable();

        Set<String> dataColumns = new HashSet<>();

        for (CellKey cellKey : cellKeys) {
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

        List<PersistentValue> dummyPersistentValues = dataManager.getDummyValues(firstCellKey.getSchema(), firstCellKey.getTable()).stream()
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
