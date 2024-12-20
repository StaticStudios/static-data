package net.staticstudios.data.misc;

import com.zaxxer.hikari.HikariConfig;
import net.staticstudios.data.DataManager;
import net.staticstudios.data.mock.Player;
import net.staticstudios.utils.ThreadUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.util.LinkedList;
import java.util.List;

public class DataTest {
    public static final int NUM_ENVIRONMENTS = 1;
    private List<MockEnvironment> mockEnvironments;

    @BeforeEach
    public void setupMockEnvironments() {
        mockEnvironments = new LinkedList<>();
        ThreadUtils.setProvider(new MockThreadProvider());
        for (int i = 0; i < NUM_ENVIRONMENTS; i++) {
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
            dataManager.loadAll(Player.class);

            MockEnvironment mockEnvironment = new MockEnvironment(hikariConfig, dataManager);
            mockEnvironments.add(mockEnvironment);
        }
    }

    @AfterEach
    public void teardownThreadUtils() {
        ThreadUtils.shutdown();
    }

    public int getNumEnvironments() {
        return NUM_ENVIRONMENTS;
    }

    public List<MockEnvironment> getMockEnvironments() {
        return mockEnvironments;
    }

}
