package net.staticstudios.data;

import net.staticstudios.data.misc.DataTest;
import net.staticstudios.data.misc.MockEnvironment;
import net.staticstudios.data.mock.reference.SnapchatUser;
import net.staticstudios.data.mock.reference.SnapchatUserSettings;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.RetryingTest;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

public class ReferenceTest extends DataTest {
    @BeforeEach
    public void init() {
        try (Statement statement = getConnection().createStatement()) {
            statement.executeUpdate("""
                    drop schema if exists snapchat cascade;
                    create schema if not exists snapchat;
                    create table if not exists snapchat.users (
                        id uuid primary key,
                        favorite_user_id uuid
                    );
                    create table if not exists snapchat.user_settings (
                        user_id uuid primary key,
                        enable_friend_requests boolean not null
                    );
                    """);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        getMockEnvironments().forEach(env -> {
            DataManager dataManager = env.dataManager();
            dataManager.loadAll(SnapchatUser.class);
        });
    }

    @RetryingTest(5)
    public void testSimpleReference() {
        MockEnvironment environment = getMockEnvironments().getFirst();
        DataManager dataManager = environment.dataManager();

        SnapchatUser user = SnapchatUser.createSync(dataManager);

        assertTrue(user.getSettings().getEnableFriendRequests());

        user.getSettings().setEnableFriendRequests(false);
        assertFalse(user.getSettings().getEnableFriendRequests());
        assertEquals(user, user.getSettings().getUser());
    }

    @RetryingTest(5)
    public void testLoadingReference() {
        try (Statement statement = getConnection().createStatement()) {
            statement.executeUpdate("""
                    insert into snapchat.users (id) values ('00000000-0000-0000-0000-000000000001');
                    insert into snapchat.user_settings (user_id, enable_friend_requests) values ('00000000-0000-0000-0000-000000000001', false);
                    """);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        MockEnvironment environment = createMockEnvironment();
        DataManager dataManager = environment.dataManager();
        dataManager.loadAll(SnapchatUser.class);

        SnapchatUserSettings settings = dataManager.get(SnapchatUserSettings.class, UUID.fromString("00000000-0000-0000-0000-000000000001"));
        assertNotNull(settings);
        SnapchatUser user = dataManager.get(SnapchatUser.class, UUID.fromString("00000000-0000-0000-0000-000000000001"));
        assertFalse(user.getSettings().getEnableFriendRequests());
    }

    @Test
    public void testSetReference() {
        MockEnvironment environment = getMockEnvironments().getFirst();
        DataManager dataManager = environment.dataManager();

        SnapchatUser user = SnapchatUser.createSync(dataManager);
        SnapchatUser favoriteUser = SnapchatUser.createSync(dataManager);

        assertNull(user.getFavoriteUser());
        user.setFavoriteUser(favoriteUser);
        assertEquals(favoriteUser, user.getFavoriteUser());

        user.setFavoriteUser(null);
        assertNull(user.getFavoriteUser());

        waitForDataPropagation();

        try (Statement statement = getConnection().createStatement()) {
            statement.executeUpdate("update snapchat.users set favorite_user_id = '" + favoriteUser.getId() + "' where id = '" + user.getId() + "'");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        waitForDataPropagation();

        assertEquals(favoriteUser, user.getFavoriteUser());
    }
}