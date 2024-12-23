package net.staticstudios.data.impl.pg;

import com.google.gson.Gson;
import com.impossibl.postgres.api.jdbc.PGConnection;
import com.impossibl.postgres.api.jdbc.PGNotificationListener;
import com.zaxxer.hikari.HikariConfig;
import net.staticstudios.data.DataManager;
import net.staticstudios.utils.ShutdownStage;
import net.staticstudios.utils.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.function.Consumer;

public class PostgresListener {
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSX");
    public static String CREATE_DATA_NOTIFY_FUNCTION = """
            create or replace function propagate_data_update() returns trigger as $$
            declare
                notification text;
            begin
                notification := to_char(current_timestamp AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.FF6"Z"') || ',' || tg_table_schema || ',' || TG_TABLE_NAME || ',' || TG_OP || ',' || current_setting('application_name') || ',' ||
                                json_build_object(
                                    'old', (case when TG_OP = 'INSERT' then '{}' else row_to_json(OLD) end),
                                    'new', (case when TG_OP = 'DELETE' then '{}' else row_to_json(NEW) end)
                                )::text;
            
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
                    AND tgrelid = '%s'::regclass
                ) THEN
                    CREATE TRIGGER propagate_data_update_trigger
                    AFTER INSERT OR UPDATE OR DELETE ON %s
                    FOR EACH ROW EXECUTE PROCEDURE propagate_data_update();
                END IF;
            END;
            $$
            """;
    private final Logger logger = LoggerFactory.getLogger(PostgresListener.class);
    private final Set<String> tablesTriggered = Collections.synchronizedSet(new HashSet<>());
    private final ConcurrentLinkedDeque<Consumer<PostgresNotification>> notificationHandlers = new ConcurrentLinkedDeque<>();
    private final PGConnection pgConnection;
    private final Gson gson = new Gson();

    public PostgresListener(DataManager dataManager, HikariConfig hikariConfig) {
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
                logger.trace("Creating data_notify function");
                statement.execute(CREATE_DATA_NOTIFY_FUNCTION);
            }

            pgConnection.addNotificationListener("data_notification", new PGNotificationListener() {
                @Override
                public void notification(int processId, String channelName, String payload) {
                    logger.trace("Received notification. PID: {}, Channel: {}, Payload: {}", processId, channelName, payload);
                    String[] parts = payload.split(",", 6);
                    String encodedTimestamp = parts[0];
                    String schema = parts[1];
                    String table = parts[2];
                    String encodedOperation = parts[3];
                    String applicationName = parts[4];
                    String encodedData = parts[5];

                    //Filter out notifications from this application (data manager session)
                    if (dataManager.getApplicationName().equals(applicationName)) {
                        logger.trace("Ignoring notification from this session");
                        return;
                    }

                    PostgresData data = gson.fromJson(encodedData, PostgresData.class);

                    OffsetDateTime offsetDateTime = OffsetDateTime.parse(encodedTimestamp, DATE_TIME_FORMATTER);

                    PostgresNotification notification = new PostgresNotification(
                            offsetDateTime.toInstant(),
                            schema,
                            table,
                            PostgresOperation.valueOf(encodedOperation),
                            data
                    );

                    for (Consumer<PostgresNotification> handler : notificationHandlers) {
                        try {
                            handler.accept(notification);
                        } catch (Exception e) {
                            logger.error("Error handling notification", e);
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
        logger.debug("Notification listener started");

        ThreadUtils.onShutdownRunSync(ShutdownStage.CLEANUP, () -> {
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
     * @param connection  the connection to the database
     * @param schemaTable the table to add the trigger to
     */
    public void ensureTableHasTrigger(Connection connection, String schemaTable) {
        if (tablesTriggered.contains(schemaTable)) {
            return;
        }

        String sql = CREATE_TRIGGER.formatted(schemaTable, schemaTable);
        logger.debug("Adding propagate_data_update_trigger to table: {}", schemaTable);

        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        tablesTriggered.add(schemaTable);
    }
}
