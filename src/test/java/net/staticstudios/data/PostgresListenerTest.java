package net.staticstudios.data;

import net.staticstudios.data.impl.pg.PostgresListener;
import net.staticstudios.data.impl.pg.PostgresNotification;
import net.staticstudios.data.impl.pg.PostgresOperation;
import net.staticstudios.data.misc.DataTest;
import net.staticstudios.data.misc.MockEnvironment;
import org.junit.jupiter.api.BeforeEach;
import org.junitpioneer.jupiter.RetryingTest;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

public class PostgresListenerTest extends DataTest {
    @BeforeEach
    public void init() {
        try (Statement statement = getConnection().createStatement()) {
            statement.executeUpdate("""
                    drop schema if exists test cascade;
                    create schema if not exists test;
                    create table if not exists test.test (
                        id uuid primary key,
                        value int not null
                    );
                    """);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @RetryingTest(5)
    public void testInsertUpdateDelete() {
        MockEnvironment environment = getMockEnvironments().getFirst();
        DataManager dataManager = environment.dataManager();
        PostgresListener pgListener = dataManager.getPostgresListener();

        CompletableFuture<PostgresNotification> insertFuture = new CompletableFuture<>();
        CompletableFuture<PostgresNotification> updateFuture = new CompletableFuture<>();
        CompletableFuture<PostgresNotification> deleteFuture = new CompletableFuture<>();

        pgListener.ensureTableHasTrigger(getConnection(), "test.test");

        pgListener.addHandler(notification -> {
            if (notification.getSchema().equals("test")) {
                if (notification.getOperation() == PostgresOperation.INSERT) {
                    insertFuture.complete(notification);
                } else if (notification.getOperation() == PostgresOperation.UPDATE) {
                    updateFuture.complete(notification);
                } else if (notification.getOperation() == PostgresOperation.DELETE) {
                    deleteFuture.complete(notification);
                }
            }
        });

        UUID id = UUID.randomUUID();
        try (Statement statement = getConnection().createStatement()) {
            statement.executeUpdate("insert into test.test (id, value) values ('" + id + "', 1)");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        PostgresNotification insertNotification = insertFuture.join();
        assertEquals(id, UUID.fromString(insertNotification.getData().newDataValueMap().get("id")));
        assertTrue(insertNotification.getData().oldDataValueMap().isEmpty());
        assertEquals(1, Integer.valueOf(insertNotification.getData().newDataValueMap().get("value")));

        try (Statement statement = getConnection().createStatement()) {
            statement.executeUpdate("update test.test set value = 2 where id = '" + id + "'");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        PostgresNotification updateNotification = updateFuture.join();
        assertEquals(id, UUID.fromString(updateNotification.getData().newDataValueMap().get("id")));
        assertEquals(1, Integer.valueOf(updateNotification.getData().oldDataValueMap().get("value")));
        assertEquals(2, Integer.valueOf(updateNotification.getData().newDataValueMap().get("value")));

        try (Statement statement = getConnection().createStatement()) {
            statement.executeUpdate("delete from test.test where id = '" + id + "'");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        PostgresNotification deleteNotification = deleteFuture.join();
        assertEquals(id, UUID.fromString(deleteNotification.getData().oldDataValueMap().get("id")));
        assertTrue(deleteNotification.getData().newDataValueMap().isEmpty());
        assertEquals(2, Integer.valueOf(deleteNotification.getData().oldDataValueMap().get("value")));
    }

    @RetryingTest(5)
    public void testReConnect() throws InterruptedException, SQLException {
        MockEnvironment environment = getMockEnvironments().getFirst();
        DataManager dataManager = environment.dataManager();
        PostgresListener pgListener = dataManager.getPostgresListener();

        assertFalse(pgListener.pgConnection.isClosed());
        pgListener.pgConnection.close();
        assertTrue(pgListener.pgConnection.isClosed());

        Thread.sleep(2000);

        assertFalse(pgListener.pgConnection.isClosed());
    }
}