package net.staticstudios.data;

import com.zaxxer.hikari.HikariConfig;
import net.staticstudios.utils.ThreadUtils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class PlayerTest {

    @Test
    public void createPlayer() throws InterruptedException {
        ThreadUtils.setProvider(new MockThreadProvider());
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setDataSourceClassName("com.impossibl.postgres.jdbc.PGDataSource");
        hikariConfig.addDataSourceProperty("serverName", "localhost");
        hikariConfig.addDataSourceProperty("portNumber", 12345);
        hikariConfig.addDataSourceProperty("user", "postgres");
        hikariConfig.addDataSourceProperty("password", "password");
        hikariConfig.addDataSourceProperty("databaseName", "postgres");
        hikariConfig.setLeakDetectionThreshold(10000);
        hikariConfig.setMaximumPoolSize(10);


        DataManager dataManager = new DataManager(hikariConfig);
        System.out.println(dataManager.loadAll(Player.class));

        Player player = Player.create(dataManager, "Test");

        assertEquals(player.getName(), "Test");

        player.setName("Test2");
        assertEquals(player.getName(), "Test2");
        player.setName(null);
        assertNull(player.getName());
        player.setName("Test2");
        assertEquals(player.getName(), "Test2");

        player.setNickname("TestNick");
        assertEquals(player.getNickname(), "TestNick");

        assertNull(player.getBackpack());
        Backpack backpack = Backpack.create(dataManager, 10);
        player.setBackpack(backpack);
        assertEquals(player.getBackpack(), backpack);
        backpack.setSize(20);
        assertEquals(backpack.getSize(), 20);
        assertEquals(player.getBackpack().getSize(), 20);


        assertTrue(player.getHomeLocations().isEmpty());
        assertTrue(player.getFavoriteNumbers().isEmpty());

        player.getFavoriteNumbers().add(1);
        player.getFavoriteNumbers().add(2);

        assertEquals(player.getFavoriteNumbers().size(), 2);
        assertEquals(player.getFavoriteNumbers().iterator().next(), 1);


        player.getHomeLocations().add(HomeLocation.create(dataManager, 1, 2, 3));
        player.getHomeLocations().add(HomeLocation.create(dataManager, 4, 5, 6));


        assertEquals(player.getHomeLocations().size(), 2);


        assertEquals(player.getHomeLocations().iterator().next().getZ(), 3);


        Island island = Island.create(dataManager, "TestIsland");
        assertTrue(island.getMembers().isEmpty());
        island.getMembers().add(player);


        dataManager.dump();

        assertEquals(island.getMembers().size(), 1);
        assertEquals(player, island.getMembers().iterator().next());

        assertEquals(player.getIsland(), island);

        //todo: when a player has their island id set, the collection should be updated

        Thread.sleep(500);

        //todo: read the db and check the values

        ThreadUtils.shutdown();
    }
}
