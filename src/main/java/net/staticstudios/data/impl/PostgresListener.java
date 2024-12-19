package net.staticstudios.data.impl;

import com.impossibl.postgres.api.jdbc.PGConnection;
import com.impossibl.postgres.api.jdbc.PGNotificationListener;
import com.zaxxer.hikari.HikariConfig;
import net.staticstudios.utils.ShutdownStage;
import net.staticstudios.utils.ThreadUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.function.Consumer;

public class PostgresListener {
    public static String CREATE_DATA_NOTIFY_FUNCTION = """
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
    public static String CREATE_TRIGGER = """
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


    private final Set<String> tablesTriggered = Collections.synchronizedSet(new HashSet<>());
    private final ConcurrentLinkedDeque<Consumer<PostgresNotification>> notificationHandlers = new ConcurrentLinkedDeque<>();
    private final PGConnection pgConnection;

    public PostgresListener(HikariConfig hikariConfig) {
        try {
            Class.forName("com.impossibl.postgres.jdbc.PGDriver");

            Properties props = hikariConfig.getDataSourceProperties();
            String hostname = props.getProperty("serverName");
            int port = (int) props.get("portNumber");
            String user = props.getProperty("user");
            String password = props.getProperty("password");
            String database = props.getProperty("databaseName");
            this.pgConnection = DriverManager.getConnection("jdbc:pgsql://" + hostname + ":" + port + "/" + database, user, password).unwrap(PGConnection.class);

            try (Statement statement = pgConnection.createStatement()) {
                System.out.println("Creating data_notify function");
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

                    PostgresNotification notification = null;

                    for (Consumer<PostgresNotification> handler : notificationHandlers) {
                        try {
                            handler.accept(notification);
                        } catch (Exception e) {
                            //todo: better logging
                            e.printStackTrace();
                        }
                    }


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
    }

    public void addHandler(Consumer<PostgresNotification> handler) {
        notificationHandlers.add(handler);
    }


    /**
     * Whenever we see a new table, make sure the trigger is added to it
     *
     * @param connection the connection to the database
     * @param table      the table to add the trigger to
     */
    public void ensureTableHasTrigger(Connection connection, String table) {
        if (tablesTriggered.contains(table)) {
            return;
        }

        String sql = CREATE_TRIGGER.formatted(table);
        System.out.println("Adding propagate_data_update_trigger to table: " + table);

        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        tablesTriggered.add(table);
    }
}
