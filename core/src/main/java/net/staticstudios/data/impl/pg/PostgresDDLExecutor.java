package net.staticstudios.data.impl.pg;

import net.staticstudios.data.util.ConnectionConsumer;
import org.intellij.lang.annotations.Language;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Serializes Static Data DDL across every process connected to the same PostgreSQL database.
 */
public final class PostgresDDLExecutor {
    // ASCII for "STATICDL". Advisory lock keys have their own namespace in PostgreSQL.
    private static final long LOCK_ID = 0x535441544943444CL;
    @Language("SQL")
    private static final String LOCK_SQL = "SELECT pg_advisory_lock(?)";
    @Language("SQL")
    private static final String UNLOCK_SQL = "SELECT pg_advisory_unlock(?)";

    private PostgresDDLExecutor() {
    }

    public static void execute(Connection connection, ConnectionConsumer ddl) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(LOCK_SQL)) {
            statement.setLong(1, LOCK_ID);
            statement.execute();
        }

        Throwable failure = null;
        try {
            ddl.accept(connection);
        } catch (SQLException | RuntimeException | Error e) {
            failure = e;
            throw e;
        } finally {
            try (PreparedStatement statement = connection.prepareStatement(UNLOCK_SQL)) {
                statement.setLong(1, LOCK_ID);
                statement.execute();
            } catch (SQLException unlockFailure) {
                if (failure == null) {
                    throw unlockFailure;
                }
                failure.addSuppressed(unlockFailure);
            }
        }
    }
}
