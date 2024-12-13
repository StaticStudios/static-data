package net.staticstudios.data.impl;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.data.InitialPersistentData;
import net.staticstudios.data.data.PersistentData;
import net.staticstudios.data.data.UniqueData;
import net.staticstudios.data.key.CellKey;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class PersistentDataManager implements DataTypeManager<PersistentData<?>, InitialPersistentData> {

    private final DataManager dataManager;
    private final PostgresListener pgListener;

    public PersistentDataManager(DataManager dataManager, PostgresListener pgListener) {
        this.dataManager = dataManager;
        this.pgListener = pgListener;

        pgListener.addHandler(notification -> {
            //todo: something
        });
    }

    public void setInDataSource(List<InitialPersistentData> initialData) throws Exception {
        try (Connection connection = dataManager.getConnection()) {
            boolean autoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            for (InitialPersistentData initial : initialData) {
                PersistentData<?> data = initial.getData();
                pgListener.ensureTableHasTrigger(connection, data.getTable());
                String schema = data.getSchema();
                String table = data.getTable();
                String column = data.getColumn();
                String idColumn = data.getIdColumn();

                String sql = "INSERT INTO " + schema + "." + table + " (" + column + ", " + idColumn + ") VALUES (?, ?) ON CONFLICT (" + idColumn + ") DO UPDATE SET " + column + " = EXCLUDED." + column;
                System.out.println(sql);


                //todo: log

                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    //todo: use value serializers
                    statement.setObject(1, initial.getValue());
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
        if (idColumn.equals(dummyHolder.getPKey().getColumn())) {
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
            sqlBuilder.append(dummyHolder.getPKey().getColumn());

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
            sqlBuilder.append(dummyHolder.getPKey().getColumn());
            sql = sqlBuilder.toString();
        }

        System.out.println(sql);

        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                UUID id = resultSet.getObject(dummyHolder.getPKey().getColumn(), UUID.class);
                for (String column : dataColumns) {
                    Object value = resultSet.getObject(column);
                    Object deserialized = value; //todo: deserialize
                    dataManager.cache(new CellKey(firstCellKey.getSchema(), firstCellKey.getTable(), column, id, idColumn), deserialized);
                }
            }
        }
    }
}
