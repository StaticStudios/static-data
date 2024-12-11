package net.staticstudios.data;

import com.zaxxer.hikari.HikariConfig;
import net.staticstudios.data.v2.DataManager;
import net.staticstudios.utils.ThreadUtils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
        hikariConfig.setLeakDetectionThreshold(10000);
        hikariConfig.setMaximumPoolSize(10);

        DataManager dataManager = new DataManager(hikariConfig);
        Player player = Player.create(dataManager, "Test");

        assertEquals(player.getName(), "Test");

        player.setName("Test2");
        assertEquals(player.getName(), "Test2");

        assertEquals(player.getBackpackSize(), 9);

        Thread.sleep(3000);

        ThreadUtils.shutdown();
    }
}
