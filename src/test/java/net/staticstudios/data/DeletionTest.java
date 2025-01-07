package net.staticstudios.data;

import net.staticstudios.data.data.collection.SimplePersistentCollection;
import net.staticstudios.data.key.RedisKey;
import net.staticstudios.data.misc.DataTest;
import net.staticstudios.data.misc.MockEnvironment;
import net.staticstudios.data.misc.TestUtils;
import net.staticstudios.data.mock.deletions.*;
import org.junit.jupiter.api.BeforeEach;
import org.junitpioneer.jupiter.RetryingTest;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;

import static org.junit.jupiter.api.Assertions.*;

public class DeletionTest extends DataTest {
    @BeforeEach
    public void init() {
        try (Statement statement = getConnection().createStatement()) {
            statement.executeUpdate("""
                    drop schema if exists minecraft cascade;
                    create schema if not exists minecraft;
                    create table if not exists minecraft.users (
                        id uuid primary key,
                        name text not null
                    );
                    create table if not exists minecraft.user_meta (
                        id uuid primary key,
                        account_creation timestamp not null
                    );
                    create table if not exists minecraft.user_stats (
                        id uuid primary key
                    );
                    create table if not exists minecraft.servers (
                        id uuid primary key,
                        name text not null
                    );
                    create table if not exists minecraft.skins (
                        id uuid primary key,
                        user_id uuid,
                        name text not null
                    );
                    create table if not exists minecraft.user_servers (
                        user_id uuid not null,
                        server_id uuid not null,
                        primary key (user_id, server_id)
                    );
                    create table if not exists minecraft.worlds (
                        id uuid primary key,
                        user_id uuid not null,
                        name text not null
                    );
                    """);
            getJedis().del("*");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @RetryingTest(5)
    public void testCascadeDeletionStrategy() {
        getMockEnvironments().forEach(env -> {
            DataManager dataManager = env.dataManager();
            dataManager.loadAll(MinecraftUserWithCascadeDeletionStrategy.class);
        });

        MockEnvironment environment = getMockEnvironments().getFirst();
        DataManager dataManager = environment.dataManager();

        int initialCacheSize = dataManager.getCacheSize();

        MinecraftUserWithCascadeDeletionStrategy user = MinecraftUserWithCascadeDeletionStrategy.createSync(dataManager, "Steve", "0.0.0.0");
        MinecraftServer server1 = MinecraftServer.createSync(dataManager, "Server 1");
        MinecraftServer server2 = MinecraftServer.createSync(dataManager, "Server 2");
        MinecraftSkin skin1 = MinecraftSkin.createSync(dataManager, "Skin 1");
        MinecraftSkin skin2 = MinecraftSkin.createSync(dataManager, "Skin 2");

        user.servers.add(server1);
        user.servers.add(server2);
        user.skins.add(skin1);
        user.skins.add(skin2);
        user.worldNames.add("World 1");
        user.worldNames.add("World 2");

        waitForDataPropagation();

        //Validate the data exists in the database
        try (Statement statement = getConnection().createStatement()) {
            assertEquals(1, TestUtils.getResultCount(statement.executeQuery("select * from minecraft.users where id = '" + user.getId() + "'")));
            assertEquals(1, TestUtils.getResultCount(statement.executeQuery("select * from minecraft.user_meta where id = '" + user.getId() + "'")));
            assertEquals(1, TestUtils.getResultCount(statement.executeQuery("select * from minecraft.user_stats where id = '" + user.getId() + "'")));
            assertEquals(2, TestUtils.getResultCount(statement.executeQuery("select * from minecraft.servers join minecraft.user_servers on servers.id = user_servers.server_id where user_servers.user_id = '" + user.getId() + "'")));
            assertEquals(2, TestUtils.getResultCount(statement.executeQuery("select * from minecraft.skins where user_id = '" + user.getId() + "'")));
            assertEquals(2, TestUtils.getResultCount(statement.executeQuery("select * from minecraft.worlds where user_id = '" + user.getId() + "'")));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        RedisKey rs = user.ipAddress.getKey();
        assertEquals("0.0.0.0", getJedis().get(rs.toString()));
        dataManager.delete(user);

        assertNull(dataManager.get(MinecraftUserWithCascadeDeletionStrategy.class, user.getId()));
        assertNull(dataManager.get(MinecraftUserStatistics.class, user.getId()));
        assertNull(dataManager.get(MinecraftServer.class, server1.getId()));
        assertNull(dataManager.get(MinecraftServer.class, server2.getId()));
        assertNull(dataManager.get(MinecraftSkin.class, skin1.getId()));
        assertNull(dataManager.get(MinecraftSkin.class, skin2.getId()));
        assertEquals(0, dataManager.getPersistentCollectionManager().getCollectionEntries((SimplePersistentCollection<?>) user.worldNames).size());

        assertEquals(initialCacheSize, dataManager.getCacheSize());

        waitForDataPropagation();

        try (Statement statement = getConnection().createStatement()) {
            assertEquals(0, TestUtils.getResultCount(statement.executeQuery("select * from minecraft.users")));
            assertEquals(0, TestUtils.getResultCount(statement.executeQuery("select * from minecraft.user_meta")));
            assertEquals(0, TestUtils.getResultCount(statement.executeQuery("select * from minecraft.user_stats")));
            assertEquals(0, TestUtils.getResultCount(statement.executeQuery("select * from minecraft.user_servers")));
            assertEquals(0, TestUtils.getResultCount(statement.executeQuery("select * from minecraft.servers")));
            assertEquals(0, TestUtils.getResultCount(statement.executeQuery("select * from minecraft.skins")));
            assertEquals(0, TestUtils.getResultCount(statement.executeQuery("select * from minecraft.worlds")));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        assertEquals(0, getJedis().keys(rs.toString()).size());
    }

    @RetryingTest(5)
    public void testNoActionDeletionStrategy() {
        getMockEnvironments().forEach(env -> {
            DataManager dataManager = env.dataManager();
            dataManager.loadAll(MinecraftUserWithNoActionDeletionStrategy.class);
        });

        MockEnvironment environment = getMockEnvironments().getFirst();
        DataManager dataManager = environment.dataManager();

        MinecraftUserWithNoActionDeletionStrategy user = MinecraftUserWithNoActionDeletionStrategy.createSync(dataManager, "Steve", "0.0.0.0");
        MinecraftUserStatistics stats = user.statistics.get();
        Timestamp accountCreation = user.accountCreation.get();
        MinecraftServer server1 = MinecraftServer.createSync(dataManager, "Server 1");
        MinecraftServer server2 = MinecraftServer.createSync(dataManager, "Server 2");
        MinecraftSkin skin1 = MinecraftSkin.createSync(dataManager, "Skin 1");
        MinecraftSkin skin2 = MinecraftSkin.createSync(dataManager, "Skin 2");

        user.servers.add(server1);
        user.servers.add(server2);
        user.skins.add(skin1);
        user.skins.add(skin2);
        user.worldNames.add("World 1");
        user.worldNames.add("World 2");

        waitForDataPropagation();

        try (Statement statement = getConnection().createStatement()) {
            assertEquals(1, TestUtils.getResultCount(statement.executeQuery("select * from minecraft.users where id = '" + user.getId() + "'")));
            assertEquals(1, TestUtils.getResultCount(statement.executeQuery("select * from minecraft.user_meta where id = '" + user.getId() + "'")));
            assertEquals(1, TestUtils.getResultCount(statement.executeQuery("select * from minecraft.user_stats where id = '" + user.getId() + "'")));
            assertEquals(2, TestUtils.getResultCount(statement.executeQuery("select * from minecraft.servers join minecraft.user_servers on servers.id = user_servers.server_id where user_servers.user_id = '" + user.getId() + "'")));
            assertEquals(2, TestUtils.getResultCount(statement.executeQuery("select * from minecraft.skins where user_id = '" + user.getId() + "'")));
            assertEquals(2, TestUtils.getResultCount(statement.executeQuery("select * from minecraft.worlds where user_id = '" + user.getId() + "'")));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        RedisKey redisKey = user.ipAddress.getKey();
        assertEquals("0.0.0.0", getJedis().get(redisKey.toString()));

        dataManager.delete(user);

        assertNull(dataManager.get(MinecraftUserWithNoActionDeletionStrategy.class, user.getId()));
        assertEquals(stats, dataManager.get(MinecraftUserStatistics.class, user.getId()));
        assertEquals(2, user.servers.size());
        assertEquals(2, user.skins.size());
        assertEquals(2, user.worldNames.size());
        assertEquals("0.0.0.0", user.ipAddress.get());

        waitForDataPropagation();

        try (Statement statement = getConnection().createStatement()) {
            assertEquals(0, TestUtils.getResultCount(statement.executeQuery("select * from minecraft.users where id = '" + user.getId() + "'")));
            assertEquals(1, TestUtils.getResultCount(statement.executeQuery("select * from minecraft.user_meta where id = '" + user.getId() + "'")));
            assertEquals(1, TestUtils.getResultCount(statement.executeQuery("select * from minecraft.user_stats where id = '" + user.getId() + "'")));
            assertEquals(2, TestUtils.getResultCount(statement.executeQuery("select * from minecraft.servers join minecraft.user_servers on servers.id = user_servers.server_id where user_servers.user_id = '" + user.getId() + "'")));
            assertEquals(2, TestUtils.getResultCount(statement.executeQuery("select * from minecraft.skins where user_id = '" + user.getId() + "'")));
            assertEquals(2, TestUtils.getResultCount(statement.executeQuery("select * from minecraft.worlds where user_id = '" + user.getId() + "'")));

            ResultSet rs;
            rs = statement.executeQuery("select * from minecraft.user_meta where id = '" + user.getId() + "'");
            rs.next();
            assertEquals(accountCreation.toInstant().toEpochMilli(), rs.getTimestamp("account_creation").toInstant().toEpochMilli()); //Use mills to account for different amounts of precision
            rs.close();
            rs = statement.executeQuery("select * from minecraft.user_stats where id = '" + user.getId() + "'");
            rs.next();
            assertEquals(user.getId(), rs.getObject("id"));
            rs.close();
            rs = statement.executeQuery("select * from minecraft.servers join minecraft.user_servers on servers.id = user_servers.server_id where user_servers.user_id = '" + user.getId() + "'");
            rs.next();
            assertEquals(server1.getId(), rs.getObject("id"));
            rs.next();
            assertEquals(server2.getId(), rs.getObject("id"));
            rs.close();
            rs = statement.executeQuery("select * from minecraft.skins where user_id = '" + user.getId() + "'");
            rs.next();
            assertEquals(skin1.getId(), rs.getObject("id"));
            rs.next();
            assertEquals(skin2.getId(), rs.getObject("id"));
            rs.close();
            rs = statement.executeQuery("select * from minecraft.worlds where user_id = '" + user.getId() + "'");
            rs.next();
            assertEquals("World 1", rs.getString("name"));
            rs.next();
            assertEquals("World 2", rs.getString("name"));
            rs.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        assertEquals("0.0.0.0", getJedis().get(redisKey.toString()));
    }

    @RetryingTest(5)
    public void testUnlinkDeletionStrategy() {
        //Note that DeletionStrategy.UNLINK acts like DeletionStrategy.CASCADE, for everything but PersistentUniqueDataCollections & PersistentManyToManyCollections
        getMockEnvironments().forEach(env -> {
            DataManager dataManager = env.dataManager();
            dataManager.loadAll(MinecraftUserWithUnlinkDeletionStrategy.class);
        });

        MockEnvironment environment = getMockEnvironments().getFirst();
        DataManager dataManager = environment.dataManager();

        MinecraftUserWithUnlinkDeletionStrategy user = MinecraftUserWithUnlinkDeletionStrategy.createSync(dataManager, "Steve", "0.0.0.0");
        MinecraftServer server1 = MinecraftServer.createSync(dataManager, "Server 1");
        MinecraftServer server2 = MinecraftServer.createSync(dataManager, "Server 2");
        MinecraftSkin skin1 = MinecraftSkin.createSync(dataManager, "Skin 1");
        MinecraftSkin skin2 = MinecraftSkin.createSync(dataManager, "Skin 2");

        user.servers.add(server1);
        user.servers.add(server2);
        user.skins.add(skin1);
        user.skins.add(skin2);
        user.worldNames.add("World 1");
        user.worldNames.add("World 2");

        waitForDataPropagation();

        try (Statement statement = getConnection().createStatement()) {
            assertEquals(1, TestUtils.getResultCount(statement.executeQuery("select * from minecraft.users where id = '" + user.getId() + "'")));
            assertEquals(1, TestUtils.getResultCount(statement.executeQuery("select * from minecraft.user_meta where id = '" + user.getId() + "'")));
            assertEquals(1, TestUtils.getResultCount(statement.executeQuery("select * from minecraft.user_stats where id = '" + user.getId() + "'")));
            assertEquals(2, TestUtils.getResultCount(statement.executeQuery("select * from minecraft.servers join minecraft.user_servers on servers.id = user_servers.server_id where user_servers.user_id = '" + user.getId() + "'")));
            assertEquals(2, TestUtils.getResultCount(statement.executeQuery("select * from minecraft.skins where user_id = '" + user.getId() + "'")));
            assertEquals(2, TestUtils.getResultCount(statement.executeQuery("select * from minecraft.worlds where user_id = '" + user.getId() + "'")));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        RedisKey redisKey = user.ipAddress.getKey();
        assertEquals("0.0.0.0", getJedis().get(redisKey.toString()));

        dataManager.delete(user);

        assertNull(dataManager.get(MinecraftUserWithUnlinkDeletionStrategy.class, user.getId()));
        assertNull(dataManager.get(MinecraftUserStatistics.class, user.getId()));
        assertNotNull(dataManager.get(MinecraftServer.class, server1.getId()));
        assertNotNull(dataManager.get(MinecraftServer.class, server2.getId()));
        assertNotNull(dataManager.get(MinecraftSkin.class, skin1.getId()));
        assertNotNull(dataManager.get(MinecraftSkin.class, skin2.getId()));
        assertEquals(0, dataManager.getPersistentCollectionManager().getCollectionEntries((SimplePersistentCollection<?>) user.worldNames).size());

        waitForDataPropagation();

        try (Statement statement = getConnection().createStatement()) {
            assertEquals(0, TestUtils.getResultCount(statement.executeQuery("select * from minecraft.users where id = '" + user.getId() + "'")));
            assertEquals(0, TestUtils.getResultCount(statement.executeQuery("select * from minecraft.user_meta where id = '" + user.getId() + "'")));
            assertEquals(0, TestUtils.getResultCount(statement.executeQuery("select * from minecraft.user_stats where id = '" + user.getId() + "'")));
            assertEquals(0, TestUtils.getResultCount(statement.executeQuery("select * from minecraft.servers join minecraft.user_servers on servers.id = user_servers.server_id where user_servers.user_id = '" + user.getId() + "'")));
            assertEquals(0, TestUtils.getResultCount(statement.executeQuery("select * from minecraft.skins where user_id = '" + user.getId() + "'")));
            assertEquals(0, TestUtils.getResultCount(statement.executeQuery("select * from minecraft.worlds where user_id = '" + user.getId() + "'")));

            assertEquals(2, TestUtils.getResultCount(statement.executeQuery("select * from minecraft.servers")));
            assertEquals(2, TestUtils.getResultCount(statement.executeQuery("select * from minecraft.skins")));
            assertEquals(0, TestUtils.getResultCount(statement.executeQuery("select * from minecraft.worlds")));
            assertEquals(0, TestUtils.getResultCount(statement.executeQuery("select * from minecraft.user_servers")));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        assertEquals(0, getJedis().keys(redisKey.toString()).size());
    }


    //other tests:
    //todo: test insert
    //todo: test insertAsync
    //todo: test value serializers

}