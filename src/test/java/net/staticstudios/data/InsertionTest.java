package net.staticstudios.data;

import net.staticstudios.data.misc.DataTest;
import net.staticstudios.data.misc.MockEnvironment;
import net.staticstudios.data.misc.TestUtils;
import net.staticstudios.data.mock.insertions.TwitchChatMessage;
import net.staticstudios.data.mock.insertions.TwitchUser;
import net.staticstudios.data.util.BatchInsert;
import org.junit.jupiter.api.BeforeEach;
import org.junitpioneer.jupiter.RetryingTest;

import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class InsertionTest extends DataTest {
    @BeforeEach
    public void init() {
        try (Statement statement = getConnection().createStatement()) {
            statement.executeUpdate("""
                    drop schema if exists twitch cascade;
                    create schema if not exists twitch;
                    create table if not exists twitch.users (
                        id uuid primary key,
                        name text not null
                    );
                    create table if not exists twitch.chat_messages (
                        id uuid primary key,
                        sender_id uuid not null references twitch.users(id) on delete set null deferrable initially deferred
                    );
                    """);

            for (MockEnvironment environment : getMockEnvironments()) {
                environment.dataManager().loadAll(TwitchUser.class);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @RetryingTest(5)
    public void testBatchInsert() {
        MockEnvironment environment = getMockEnvironments().getFirst();
        DataManager dataManager = environment.dataManager();

        BatchInsert batch = dataManager.batchInsert();
        TwitchUser user = TwitchUser.enqueueCreation(batch, dataManager, "test user");
        TwitchChatMessage message1 = TwitchChatMessage.enqueueCreation(batch, dataManager, user);
        TwitchChatMessage message2 = TwitchChatMessage.enqueueCreation(batch, dataManager, user);
        batch.insert();

        assertNotNull(user.getId());
        assertNotNull(message1.getId());
        assertNotNull(message2.getId());
        assertEquals(user, message1.sender.get());
        assertEquals(user, message2.sender.get());
        assertEquals(2, user.messages.size());

        try (Statement statement = getConnection().createStatement()) {
            assertEquals(2, TestUtils.getResultCount(statement.executeQuery("select * from twitch.chat_messages where sender_id = '" + user.getId() + "'")));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

}