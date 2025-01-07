package net.staticstudios.data.impl;

import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import net.staticstudios.data.DataDoesNotExistException;
import net.staticstudios.data.DataManager;
import net.staticstudios.data.DeleteContext;
import net.staticstudios.data.InsertionStrategy;
import net.staticstudios.data.data.Data;
import net.staticstudios.data.data.UniqueData;
import net.staticstudios.data.data.value.persistent.InitialPersistentValue;
import net.staticstudios.data.data.value.persistent.PersistentValue;
import net.staticstudios.data.impl.pg.PostgresListener;
import net.staticstudios.data.impl.pg.PostgresOperation;
import net.staticstudios.data.key.CellKey;
import net.staticstudios.data.util.SQLLogger;
import net.staticstudios.utils.Pair;
import org.jetbrains.annotations.Blocking;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.*;

public class PersistentValueManager extends SQLLogger {
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

                        CellKey dataKey = new CellKey(schema, table, column, id, idColumn);
                        CellKey idKey = new CellKey(schema, table, idColumn, id, idColumn);

                        String encodedValue = newDataValueMap.get(column); //Raw value as string
                        Object rawValue = dataManager.decode(dummyPV.getDataType(), encodedValue);
                        Object deserialized = dataManager.deserialize(dummyPV.getDataType(), rawValue);

                        dataManager.cache(dataKey, dummyPV.getDataType(), deserialized, notification.getInstant());
                        dataManager.cache(idKey, UUID.class, id, notification.getInstant());
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

    public void deleteFromCache(DeleteContext context) {
        //Since the whole table is being deleted, we can just remove all the values from the cache
        Set<Pair<String, UUID>> schemaTableIdSet = new HashSet<>();
        for (Data<?> data : context.toDelete()) {
            if (data instanceof PersistentValue<?> pv) {
                schemaTableIdSet.add(Pair.of(pv.getSchema() + "." + pv.getTable(), pv.getHolder().getRootHolder().getId()));
            }
        }

        dataManager.removeFromCacheIf(key -> {
            if (!(key instanceof CellKey cellKey)) {
                return false;
            }
            return schemaTableIdSet.contains(Pair.of(cellKey.getSchema() + "." + cellKey.getTable(), cellKey.getRootHolderId()));

//            Object oldValue = dataManager.get(key);
//            try {
//                oldValue = dataManager.get(key);
//            } catch (DataDoesNotExistException ignored) {
//            }
//            dataManager.getPersistentCollectionManager().handlePersistentValueUncache(
//                    cellKey.getSchema(),
//                    cellKey.getTable(),
//                    cellKey.getColumn(),
//                    cellKey.getRootHolderId(),
//                    cellKey.getIdColumn(),
//                    oldValue
//            );
        });
    }

    @Blocking
    public void deleteFromDatabase(Connection connection, DeleteContext context) throws SQLException {
        Map<String, PersistentValue<?>> schemaTableMap = new HashMap<>();
        for (Data<?> data : context.toDelete()) {
            if (data instanceof PersistentValue<?> pv) {
                schemaTableMap.put(pv.getSchema() + "." + pv.getTable(), pv);
            }
        }

        boolean autoCommit = connection.getAutoCommit();
        connection.setAutoCommit(false);

        for (String schemaTable : schemaTableMap.keySet()) {
            PersistentValue<?> pv = schemaTableMap.get(schemaTable);
            String idColumn = pv.getIdColumn();
            String sql = "DELETE FROM " + schemaTable + " WHERE " + idColumn + " = ?";
            logSQL(sql);

            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setObject(1, pv.getHolder().getRootHolder().getId());
                statement.executeUpdate();
            }
        }

        connection.setAutoCommit(autoCommit);
    }

    public void updateCache(PersistentValue<?> persistentValue, Object value) {
        updateCache(persistentValue.getSchema(),
                persistentValue.getTable(),
                persistentValue.getColumn(),
                persistentValue.getHolder().getRootHolder().getId(),
                persistentValue.getIdColumn(),
                persistentValue.getDataType(),
                value
        );
    }

    public void updateCache(String schema, String table, String column, UUID holderId, String idColumn, Class<?> valueDataType, Object value) {
        Object oldValue = null;

        CellKey key = new CellKey(schema, table, column, holderId, idColumn);

        try {
            oldValue = dataManager.get(key);
        } catch (DataDoesNotExistException ignored) {
        }

        dataManager.cache(key, valueDataType, value, Instant.now());
        dataManager.getPersistentCollectionManager().handlePersistentValueCacheUpdated(schema, table, column, holderId, idColumn, oldValue, value);
    }

    @Blocking
    public void insertInDatabase(Connection connection, UniqueData holder, List<InitialPersistentValue> initialData) throws SQLException {
        if (initialData.isEmpty()) {
            return;
        }

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
            initialDataValues.removeIf(i -> i.getValue().getColumn().equals(idColumn));
            List<InitialPersistentValue> overwriteExisting = new ArrayList<>();
            for (InitialPersistentValue initial : initialDataValues) {
                if (initial.getValue().getInsertionStrategy() == InsertionStrategy.OVERWRITE_EXISTING) {
                    overwriteExisting.add(initial);
                }
            }

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
            sqlBuilder.append(")");

            if (!overwriteExisting.isEmpty()) {
                sqlBuilder.append(" ON CONFLICT (");
                sqlBuilder.append(idColumn);
                sqlBuilder.append(") DO UPDATE SET ");
                for (InitialPersistentValue initial : overwriteExisting) {
                    sqlBuilder.append(initial.getValue().getColumn());
                    sqlBuilder.append(" = EXCLUDED.");
                    sqlBuilder.append(initial.getValue().getColumn());
                    sqlBuilder.append(", ");
                }
                sqlBuilder.setLength(sqlBuilder.length() - 2);
            } else {
                sqlBuilder.append(" ON CONFLICT (");
                sqlBuilder.append(idColumn);
                sqlBuilder.append(") DO NOTHING");
            }
            String sql = sqlBuilder.toString();

            logSQL(sql);

            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setObject(1, holder.getId());
                int i = 2;
                for (InitialPersistentValue initial : initialDataValues) {
                    Object initialDataValue = initial.getInitialDataValue();
                    Object serialized = dataManager.serialize(initialDataValue);
                    statement.setObject(i++, serialized);
                }

                statement.executeUpdate();
            }
        }

        connection.setAutoCommit(autoCommit);
    }

    @Blocking
    public void updateInDatabase(Connection connection, PersistentValue<?> persistentValue, Object value) throws SQLException {
        String schemaTable = persistentValue.getSchema() + "." + persistentValue.getTable();
        String idColumn = persistentValue.getIdColumn();
        String column = persistentValue.getColumn();

        String sql = "UPDATE " + schemaTable + " SET " + column + " = ? WHERE " + idColumn + " = ?";
        logSQL(sql);

        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            Object serialized = dataManager.serialize(value);
            statement.setObject(1, serialized);
            statement.setObject(2, persistentValue.getHolder().getRootHolder().getId());

            statement.executeUpdate();
        }
    }

    /**
     * Column keys are expected to all be in the same table, AND are expected to all have the same id column
     */
    @SuppressWarnings("rawtypes")
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

        logSQL(sql);

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
                    dataManager.cache(new CellKey(firstCellKey.getSchema(), firstCellKey.getTable(), column, id, idColumn), dummyPV.getDataType(), deserialized, Instant.now());
                }

                if (!dataColumns.contains(idColumn)) {
                    dataManager.cache(new CellKey(firstCellKey.getSchema(), firstCellKey.getTable(), idColumn, id, idColumn), UUID.class, id, Instant.now());
                }
            }
        }
    }
}
