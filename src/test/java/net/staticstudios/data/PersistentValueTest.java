package net.staticstudios.data;

import net.staticstudios.data.misc.DataTest;
import net.staticstudios.data.misc.MockEnvironment;
import net.staticstudios.data.mock.persistentvalue.DiscordUser;
import org.junit.jupiter.api.BeforeEach;
import org.junitpioneer.jupiter.RetryingTest;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PersistentValueTest extends DataTest {

    //todo: we need to add update handlers
    //todo: we need to test what happens when we manually edit the db. do the values get set on insert, update, and delete

    @BeforeEach
    public void init() {
        try (Statement statement = getConnection().createStatement()) {
            statement.executeUpdate("""
                    drop schema if exists discord cascade;
                    create schema if not exists discord;
                    create table if not exists discord.users (
                        id uuid primary key,
                        name text not null
                    );
                    create table if not exists discord.user_settings (
                        user_id uuid primary key,
                        enable_friend_requests boolean not null default true
                    );
                    """);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        getMockEnvironments().forEach(env -> {
            DataManager dataManager = env.dataManager();
            dataManager.loadAll(DiscordUser.class);
        });
    }

    @RetryingTest(5)
    public void testSetPersistentValue() {
        MockEnvironment environment = getMockEnvironments().getFirst();
        DataManager dataManager = environment.dataManager();

        DiscordUser user = DiscordUser.createSync(dataManager, "John Doe");
        assertEquals("John Doe", user.getName());

        waitForDataPropagation();

        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("update discord.users set name = 'Jane Doe' where id = '" + user.getId() + "'");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        waitForDataPropagation();

        assertEquals("Jane Doe", user.getName());

        user.setName("User");
        assertEquals("User", user.getName());

        waitForDataPropagation();

        try (Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery("select name from discord.users where id = '" + user.getId() + "'");
            resultSet.next();
            assertEquals("User", resultSet.getString("name"));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    //todo: test setting fpv
    //todo: test null values
    //todo: test straight up deleting an fpv
}