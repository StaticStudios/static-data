package net.staticstudios.data.v2;

import com.impossibl.postgres.api.jdbc.PGConnection;
import com.impossibl.postgres.api.jdbc.PGNotificationListener;
import net.staticstudios.utils.ShutdownStage;
import net.staticstudios.utils.ThreadUtils;

import java.sql.*;
import java.util.*;

public class PersistentDataManager implements DataTypeManager<PersistentData<?>, InitialPersistentData> {
    private static String CREATE_DATA_NOTIFY_FUNCTION = """
            create or replace function propagate_data_update() returns trigger as $$
            declare
                pkey_cols text[];
                notification text;
            begin
                -- Fetch the primary key column names for the table
                select array_agg(a.attname)
                into pkey_cols
                from pg_index i
                         join pg_attribute a on a.attrelid = i.indrelid
                    AND a.attnum = ANY(i.indkey)
                where i.indrelid = TG_TABLE_NAME::regclass
                  and i.indisprimary;
            
                -- Construct the notification message
                notification := current_timestamp || ',' || tg_table_schema || ',' || TG_TABLE_NAME || ',' || TG_OP || ',' ||
                                json_build_object('pkey_cols', pkey_cols,
                                                  'data', case when TG_OP = 'DELETE' then row_to_json(OLD) else row_to_json(NEW) end)::text;
            
                -- Send the notification
                perform pg_notify('data_notification', notification);
            
                return new;
            end;
            $$ language plpgsql;
            """;
    private static String CREATE_TRIGGER = """
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1
                    FROM pg_trigger
                    WHERE tgname = 'propagate_data_update_trigger'
                ) THEN
                    CREATE TRIGGER propagate_data_update_trigger
                    AFTER INSERT OR UPDATE OR DELETE ON %s
                    FOR EACH ROW EXECUTE PROCEDURE propagate_data_update();
                END IF;
            END;
            $$
            """;
    private final DataManager dataManager;
    private final PGConnection pgConnection;
    private final Set<String> tablesTriggered = Collections.synchronizedSet(new HashSet<>());

    public PersistentDataManager(DataManager dataManager) {
        this.dataManager = dataManager;
        //todo: get the host, and port from datamanager
        try {
            Class.forName("com.impossibl.postgres.jdbc.PGDriver");

            String hostname = "localhost";
            String password = "password";
            String port = "12345";
            this.pgConnection = DriverManager.getConnection("jdbc:pgsql://" + hostname + ":" + port + "/postgres", "postgres", password).unwrap(PGConnection.class);

            try (Statement statement = pgConnection.createStatement()) {
                statement.execute(CREATE_DATA_NOTIFY_FUNCTION);
            }

            pgConnection.addNotificationListener("data_notification", new PGNotificationListener() {
                @Override
                public void notification(int processId, String channelName, String payload) {
                    System.out.printf("PID: %s, Channel: %s, Payload: %s%n", processId, channelName, payload);
                    String[] parts = payload.split(",", 5);
                    String timestamp = parts[0];
                    String schema = parts[1];
                    String table = parts[2];
                    String operation = parts[3];
                    String data = parts[4];


                }
            });

            try (Statement statement = pgConnection.createStatement()) {
                statement.execute("LISTEN data_notification");
            }
        } catch (SQLException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        ThreadUtils.onShutdownRunSync(ShutdownStage.EARLY, () -> {
            try {
                pgConnection.close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });


        //todo: subscribe to pg
    }

    public static DataKey createDataKey(String schema, String table, String column, PrimaryKey holderPkey) {
        return DataKey.of("sql", schema, table, column, holderPkey);
    }

    @Override
    public void insertIntoDataSource(UniqueData holder, List<InitialPersistentData> initialData) throws Exception {
        try (Connection connection = dataManager.getConnection()) {
            ensureTableHasTrigger(connection, holder.getTable());
            StringBuilder sqlBuilder = new StringBuilder("INSERT INTO ").append(holder.getSchema()).append(".").append(holder.getTable()).append(" (");

            List<String> columns = new ArrayList<>(Arrays.asList(holder.getPKey().getColumns()));

            for (InitialPersistentData initial : initialData) {
                PersistentData<?> data = initial.getData();
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

            sqlBuilder.append(") RETURNING *");

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

    @Override
    public List<Object> loadFromDataSource(List<PersistentData<?>> dataList) throws Exception {
        //todo: load all columns, since we will probably need them
        List<Object> dataValues = new ArrayList<>();
        try (Connection connection = dataManager.getConnection()) {
            for (PersistentData<?> data : dataList) {
                ensureTableHasTrigger(connection, data.getTable());
                String schema = data.getSchema();
                String table = data.getTable();
                String column = data.getColumn();
                PrimaryKey holderPkey = data.getHolderPrimaryKey();


                StringBuilder sqlBuilder = new StringBuilder("SELECT " + column + " FROM " + schema + "." + table + " WHERE ");
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
                    Object[] values = holderPkey.getValues();

                    for (int i = 0; i < values.length; i++) {
                        statement.setObject(i + 1, values[i]);
                    }

                    ResultSet resultSet = statement.executeQuery();
                    if (resultSet.next()) {
                        //todo: use value serializers
                        dataValues.add(resultSet.getObject(column));
                    } else {
                        throw new DataDoesNotExistException("Data does not exist in the database. " + data);
                    }
                }
            }
        }

        return dataValues;
    }

    @Override
    public void updateInDataSource(List<PersistentData<?>> dataList, Object value) throws Exception {
        try (Connection connection = dataManager.getConnection()) {
            for (PersistentData<?> data : dataList) {
                ensureTableHasTrigger(connection, data.getTable());
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

    /**
     * Whenever we see a new table, make sure the trigger is added to it
     *
     * @param connection the connection to the database
     * @param table      the table to add the trigger to
     */
    private void ensureTableHasTrigger(Connection connection, String table) {
        if (tablesTriggered.contains(table)) {
            return;
        }

        String sql = CREATE_TRIGGER.formatted(table);
        System.out.println(sql);

        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
