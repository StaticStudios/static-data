package net.staticstudios.data;

import net.staticstudios.data.misc.DataTest;
import net.staticstudios.data.mock.MockUser;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

public class PersistentValueTest extends DataTest {

    @Test
    public void test() throws SQLException {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(MockUser.class);
        MockUser mockUser = MockUser.create(dataManager, "josh");
    }
}