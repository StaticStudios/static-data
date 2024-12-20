package net.staticstudios.data;

import net.staticstudios.data.misc.DataTest;
import net.staticstudios.data.misc.MockEnvironment;
import net.staticstudios.data.mock.Backpack;
import net.staticstudios.data.mock.HomeLocation;
import net.staticstudios.data.mock.Island;
import net.staticstudios.data.mock.Player;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;

import static org.junit.jupiter.api.Assertions.*;

public class PlayerTest extends DataTest {
    @Test
    public void createPlayer() throws InterruptedException {
        MockEnvironment mockEnvironment = getMockEnvironments().getFirst();
        DataManager dataManager = mockEnvironment.dataManager();

        Player player = Player.createSync(dataManager, "Test");

        assertEquals("Test", player.getName());

        player.setName("Test2");
        assertEquals("Test2", player.getName());
        player.setName(null);
        assertNull(player.getName());
        player.setName("Test2");
        assertEquals("Test2", player.getName());

        player.setNickname("TestNick");
        assertEquals("TestNick", player.getNickname());

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
    }

    @Test
    public void testUpdatePersistentValue() throws InterruptedException {
        MockEnvironment mockEnvironment = getMockEnvironments().getFirst();
        DataManager dataManager = mockEnvironment.dataManager();

        Player player = Player.createSync(dataManager, "Test");

        try (Connection connection = dataManager.getConnection()) {
            String sql = "UPDATE public.players SET name = ? WHERE id = ?";
            PreparedStatement statement = connection.prepareStatement(sql);
            statement.setString(1, "NewName");
            statement.setObject(2, player.getId());
            statement.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        }

        Thread.sleep(500);

        assertEquals(player.getName(), "NewName");
    }
}
