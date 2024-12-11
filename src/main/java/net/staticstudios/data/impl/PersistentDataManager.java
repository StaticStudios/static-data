package net.staticstudios.data.impl;

import net.staticstudios.data.DataKey;
import net.staticstudios.data.DataManager;
import net.staticstudios.data.PrimaryKey;
import net.staticstudios.data.data.InitialPersistentData;
import net.staticstudios.data.data.PersistentData;
import net.staticstudios.data.data.UniqueData;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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

    public static DataKey createDataKey(String schema, String table, String column, PrimaryKey holderPkey) {
        return DataKey.of("sql", schema, table, column, holderPkey);
    }


    @Override
    public void insertIntoDataSource(UniqueData holder, List<InitialPersistentData> initialData) throws Exception {
        try (Connection connection = dataManager.getConnection()) {
            pgListener.ensureTableHasTrigger(connection, holder.getTable());
            StringBuilder sqlBuilder = new StringBuilder("INSERT INTO ").append(holder.getSchema()).append(".").append(holder.getTable()).append(" (");

            List<String> columns = new ArrayList<>(Arrays.asList(holder.getPKey().getColumns()));

            for (InitialPersistentData initial : initialData) {
                PersistentData<?> data = initial.getKeyed();
                columns.add(data.getColumn());
            }


            for (String column : columns) {
                sqlBuilder.append(column);
                if (columns.indexOf(column) < columns.size() - 1) {
                    sqlBuilder.append(", ");
                }
            }
            sqlBuilder.append(") VALUES (");

            for (int i = 0; i < columns.size(); i++) {
                sqlBuilder.append("?");
                if (i < columns.size() - 1) {
                    sqlBuilder.append(", ");
                }
            }

            sqlBuilder.append(") ON CONFLICT (");
            for (int i = 0; i < holder.getPKey().getColumns().length; i++) {
                sqlBuilder.append(holder.getPKey().getColumns()[i]);
                if (i < holder.getPKey().getColumns().length - 1) {
                    sqlBuilder.append(", ");
                }
            }
            sqlBuilder.append(") DO UPDATE SET ");
            for (int i = 0; i < columns.size(); i++) {
                sqlBuilder.append(columns.get(i)).append(" = EXCLUDED.").append(columns.get(i));
                if (i < columns.size() - 1) {
                    sqlBuilder.append(", ");
                }
            }

            sqlBuilder.append(" RETURNING *");

            String sql = sqlBuilder.toString();
            System.out.println(sql);

            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                List<Object> values = new ArrayList<>(Arrays.asList(holder.getPKey().getValues()));

                for (InitialPersistentData initial : initialData) {
                    Object value = initial.getValue();
                    Object serialized = value; //todo: this
                    values.add(serialized);
                }

                for (int i = 0; i < values.size(); i++) {
                    statement.setObject(i + 1, values.get(i));
                }

                ResultSet resultSet = statement.executeQuery();
                if (resultSet.next()) {

                    //Cache all values
                    for (String column : columns) {
                        Object value = resultSet.getObject(column);
                        dataManager.cache(createDataKey(holder.getSchema(), holder.getTable(), column, holder.getPKey()), value);
                    }
                }
            }
        }
    }

//    @Override
//    public List<Object> loadFromDataSource(List<PersistentData<?>> dataList) throws Exception {
//        //todo: load all columns, since we will probably need them
//        List<Object> dataValues = new ArrayList<>();
//        try (Connection connection = dataManager.getConnection()) {
//            for (PersistentData<?> data : dataList) {
//                pgListener.ensureTableHasTrigger(connection, data.getTable());
//                String schema = data.getSchema();
//                String table = data.getTable();
//                String column = data.getColumn();
//                PrimaryKey holderPkey = data.getHolderPrimaryKey();
//
//
//                StringBuilder sqlBuilder = new StringBuilder("SELECT " + column + " FROM " + schema + "." + table + " WHERE ");
//                for (int i = 0; i < holderPkey.getColumns().length; i++) {
//                    sqlBuilder.append(holderPkey.getColumns()[i]).append(" = ?");
//                    if (i < holderPkey.getColumns().length - 1) {
//                        sqlBuilder.append(" AND ");
//                    }
//                }
//
//                String sql = sqlBuilder.toString();
//                System.out.println(sql);
//                //todo: log
//
//                try (PreparedStatement statement = connection.prepareStatement(sql)) {
//                    Object[] values = holderPkey.getValues();
//
//                    for (int i = 0; i < values.length; i++) {
//                        statement.setObject(i + 1, values[i]);
//                    }
//
//                    ResultSet resultSet = statement.executeQuery();
//                    if (resultSet.next()) {
//                        //todo: use value serializers
//                        dataValues.add(resultSet.getObject(column));
//                    } else {
//                        throw new DataDoesNotExistException("Data does not exist in the database. " + data);
//                    }
//                }
//            }
//        }
//
//        return dataValues;
//    }

    @Override
    public void updateInDataSource(List<PersistentData<?>> dataList, Object value) throws Exception {
        try (Connection connection = dataManager.getConnection()) {
            for (PersistentData<?> data : dataList) {
                pgListener.ensureTableHasTrigger(connection, data.getTable());
                String schema = data.getSchema();
                String table = data.getTable();
                String column = data.getColumn();
                PrimaryKey holderPkey = data.getHolderPrimaryKey();

                StringBuilder sqlBuilder = new StringBuilder("UPDATE " + schema + "." + table + " SET " + column + " = ? WHERE ");

                for (int i = 0; i < holderPkey.getColumns().length; i++) {
                    sqlBuilder.append(holderPkey.getColumns()[i]).append(" = ?");
                    if (i < holderPkey.getColumns().length - 1) {
                        sqlBuilder.append(" AND ");
                    }
                }

                String sql = sqlBuilder.toString();
                System.out.println(sql);


                //todo: log

                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    //todo: use value serializers
                    statement.setObject(1, value);

                    Object[] pkeyValues = holderPkey.getValues();
                    for (int i = 0; i < pkeyValues.length; i++) {
                        statement.setObject(i + 2, pkeyValues[i]);
                    }


                    statement.executeUpdate();
                }
            }
        }
    }
}
