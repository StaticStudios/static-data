package net.staticstudios.data;

import net.staticstudios.data.misc.DataTest;
import net.staticstudios.data.misc.MockEnvironment;
import net.staticstudios.data.mock.persistentvalue.DiscordUser;
import net.staticstudios.data.mock.persistentvalue.DiscordUserSettings;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junitpioneer.jupiter.RetryingTest;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

public class PersistentValueTest extends DataTest {

    //todo: we need to test what happens when we manually edit the db. do the values get set on insert, update, and delete
    //todo: test default values
    //todo: test blocking #set calls

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
                    create table if not exists discord.user_meta (
                        id uuid primary key,
                        name_updates_called int not null default 0,
                        enable_friend_requests_updates_called int not null default 0
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
            dataManager.loadAll(DiscordUserSettings.class);
        });
    }

    @RetryingTest(5)
    public void testSetPersistentValue() {
        MockEnvironment environment = getMockEnvironments().getFirst();
        DataManager dataManager = environment.dataManager();

        DiscordUser user = DiscordUser.createSync(dataManager, "John Doe", UUID.randomUUID());
        assertEquals("John Doe", user.getName());

        waitForDataPropagation();

        try (Statement statement = getConnection().createStatement()) {
            statement.executeUpdate("update discord.users set name = 'Jane Doe' where id = '" + user.getId() + "'");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        waitForDataPropagation();

        assertEquals("Jane Doe", user.getName());

        user.setName("User");
        assertEquals("User", user.getName());

        waitForDataPropagation();

        try (Statement statement = getConnection().createStatement()) {
            ResultSet resultSet = statement.executeQuery("select name from discord.users where id = '" + user.getId() + "'");
            resultSet.next();
            assertEquals("User", resultSet.getString("name"));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @RetryingTest(5)
    public void testSetForeignPersistentValue() {
        MockEnvironment environment = getMockEnvironments().getFirst();
        DataManager dataManager = environment.dataManager();

        DiscordUser user = DiscordUser.createSync(dataManager, "John Doe", UUID.randomUUID());
        assertTrue(user.getEnableFriendRequests()); //defaults to true

        waitForDataPropagation();

        try (Statement statement = getConnection().createStatement()) {
            ResultSet resultSet = statement.executeQuery("select enable_friend_requests from discord.user_settings where user_id = '" + user.getId() + "'");
            resultSet.next();
            assertTrue(resultSet.getBoolean("enable_friend_requests"));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        user.setEnableFriendRequests(false);
        assertFalse(user.getEnableFriendRequests());

        waitForDataPropagation();

        try (Statement statement = getConnection().createStatement()) {
            ResultSet resultSet = statement.executeQuery("select enable_friend_requests from discord.user_settings where user_id = '" + user.getId() + "'");
            resultSet.next();
            assertFalse(resultSet.getBoolean("enable_friend_requests"));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        user.setEnableFriendRequests(true);
        assertTrue(user.getEnableFriendRequests());

        waitForDataPropagation();

        try (Statement statement = getConnection().createStatement()) {
            ResultSet resultSet = statement.executeQuery("select enable_friend_requests from discord.user_settings where user_id = '" + user.getId() + "'");
            resultSet.next();
            assertTrue(resultSet.getBoolean("enable_friend_requests"));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        try (Statement statement = getConnection().createStatement()) {
            statement.executeUpdate("update discord.user_settings set enable_friend_requests = false where user_id = '" + user.getId() + "'");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        waitForDataPropagation();

        assertFalse(user.getEnableFriendRequests());
    }

    @RetryingTest(5)
    public void testForeignPersistentValuePerferExistingInsertionStrategy() {
        UUID id = UUID.randomUUID();
        try (Statement statement = getConnection().createStatement()) {
            statement.executeUpdate("insert into discord.user_settings (user_id, enable_friend_requests) values ('" + id + "', false)");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        MockEnvironment environment = createMockEnvironment();
        DataManager dataManager = environment.dataManager();

        dataManager.loadAll(DiscordUser.class);
        dataManager.loadAll(DiscordUserSettings.class);

        assertNull(dataManager.get(DiscordUser.class, id));

        DiscordUser user = DiscordUser.createSync(dataManager, "John Doe", id);
        assertFalse(user.getEnableFriendRequests());

        user.setEnableFriendRequests(true);
        assertTrue(user.getEnableFriendRequests());

        waitForDataPropagation();

        try (Statement statement = getConnection().createStatement()) {
            ResultSet resultSet = statement.executeQuery("select enable_friend_requests from discord.user_settings where user_id = '" + id + "'");
            resultSet.next();
            assertTrue(resultSet.getBoolean("enable_friend_requests"));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @RetryingTest(5)
    @Disabled("see todo message. the datamanager no longer receives its own pg notifications, so this will never work until we implement the TODO")
    public void testSetForeignPersistentValueAndSeePersistentValueUpdate() {
        MockEnvironment environment = getMockEnvironments().getFirst();
        DataManager dataManager = environment.dataManager();

        DiscordUser user = DiscordUser.createSync(dataManager, "John Doe", UUID.randomUUID());
        assertTrue(user.getEnableFriendRequests()); //defaults to true

        //todo: the settings should be created as soon as the user is, but this is not the case since they are completely separate entities
        // this functionality needs to be added to the data manager.
        // maybe instead of having the data manager do all the work, we can let it know somehow that DiscordUserSettings should be created when a DiscordUser is created
        // currently it is being created when the postgres notification comes back altering us that the user_settings table has had a new row inserted
//        DiscordSettings settings = dataManager.getUniqueData(DiscordSettings.class, user.getId());


        waitForDataPropagation();

        DiscordUserSettings settings = dataManager.get(DiscordUserSettings.class, user.getId());

        assertTrue(settings.getEnableFriendRequests());

        user.setEnableFriendRequests(false);
        assertFalse(user.getEnableFriendRequests());
        assertFalse(settings.getEnableFriendRequests());

        settings.setEnableFriendRequests(true);
        assertTrue(user.getEnableFriendRequests());
        assertTrue(settings.getEnableFriendRequests());

        try (Statement statement = getConnection().createStatement()) {
            statement.executeUpdate("update discord.user_settings set enable_friend_requests = false where user_id = '" + user.getId() + "'");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        waitForDataPropagation();

        assertFalse(user.getEnableFriendRequests());
        assertFalse(settings.getEnableFriendRequests());
    }

    @RetryingTest(5)
    public void testPersistentValueUpdateHandlers() {
        //Note that update handlers are called async
        MockEnvironment environment = getMockEnvironments().getFirst();
        DataManager dataManager = environment.dataManager();

        DiscordUser user = DiscordUser.createSync(dataManager, "John Doe", UUID.randomUUID());
        assertEquals(0, user.getNameUpdatesCalled());
        assertEquals(0, user.getEnableFriendRequestsUpdatesCalled());

        user.setName("Jane Doe");
        waitForDataPropagation();
        assertEquals(1, user.getNameUpdatesCalled());

        user.setEnableFriendRequests(false);
        waitForDataPropagation();
        assertEquals(1, user.getEnableFriendRequestsUpdatesCalled());

        user.setEnableFriendRequests(true);
        waitForDataPropagation();
        assertEquals(2, user.getEnableFriendRequestsUpdatesCalled());

        user.setName("John Doe");
        waitForDataPropagation();
        assertEquals(2, user.getNameUpdatesCalled());

        waitForDataPropagation();

        assertEquals(2, user.getNameUpdatesCalled());
        assertEquals(2, user.getEnableFriendRequestsUpdatesCalled());

        try (Statement statement = getConnection().createStatement()) {
            statement.executeUpdate("update discord.users set name = 'Jane Doe' where id = '" + user.getId() + "'");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        try (Statement statement = getConnection().createStatement()) {
            statement.executeUpdate("update discord.user_settings set enable_friend_requests = false where user_id = '" + user.getId() + "'");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        waitForDataPropagation();

        assertEquals(3, user.getNameUpdatesCalled());
        assertEquals(3, user.getEnableFriendRequestsUpdatesCalled());
    }

    @RetryingTest(5)
    public void testLoading() {
        List<UUID> ids = new ArrayList<>();
        try (Statement statement = getConnection().createStatement()) {
            for (int i = 0; i < 10; i++) {
                UUID id = UUID.randomUUID();
                ids.add(id);
                statement.executeUpdate("insert into discord.users (id, name) values ('" + id + "', 'User " + i + "')");
                statement.executeUpdate("insert into discord.user_meta (id) values ('" + id + "')");
                statement.executeUpdate("insert into discord.user_settings (user_id) values ('" + id + "')");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        MockEnvironment environment = createMockEnvironment();
        getMockEnvironments().add(environment);

        DataManager dataManager = environment.dataManager();
        dataManager.loadAll(DiscordUser.class);

        for (UUID id : ids) {
            DiscordUser user = dataManager.get(DiscordUser.class, id);
            assertEquals("User " + ids.indexOf(id), user.getName());
        }
    }

    @RetryingTest(5)
    public void testTaskQueue() {
        MockEnvironment environment = getMockEnvironments().getFirst();
        DataManager dataManager = environment.dataManager();

        DiscordUser user = DiscordUser.createSync(dataManager, "0", UUID.randomUUID());
        for (int i = 0; i < 30; i++) {
            int name = Integer.parseInt(user.getName());
            user.setName(String.valueOf(name + 1));
        }

        waitForDataPropagation();

        try (Statement statement = getConnection().createStatement()) {
            ResultSet resultSet = statement.executeQuery("select name from discord.users where id = '" + user.getId() + "'");
            resultSet.next();
            assertEquals("30", resultSet.getString("name"));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @RetryingTest(5)
    public void testUpdateInterval() {
        MockEnvironment environment = getMockEnvironments().getFirst();
        DataManager dataManager = environment.dataManager();

        DiscordUser user = DiscordUser.createSync(dataManager, "0", UUID.randomUUID());
        user.name.updateInterval(getWaitForDataPropagationTime() * 2);
        for (int i = 0; i < 30; i++) {
            int name = Integer.parseInt(user.getName());
            user.setName(String.valueOf(name + 1));
        }

        waitForDataPropagation();

        try (Statement statement = getConnection().createStatement()) {
            ResultSet resultSet = statement.executeQuery("select name from discord.users where id = '" + user.getId() + "'");
            resultSet.next();
            assertEquals("0", resultSet.getString("name"));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        waitForDataPropagation();
        waitForDataPropagation();

        try (Statement statement = getConnection().createStatement()) {
            ResultSet resultSet = statement.executeQuery("select name from discord.users where id = '" + user.getId() + "'");
            resultSet.next();
            assertEquals("30", resultSet.getString("name"));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    //todo: test straight up deleting an fpv. ideally a data does not exist exception should be thrown when calling get on a deleted fpv
}