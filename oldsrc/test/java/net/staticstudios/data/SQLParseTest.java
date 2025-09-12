package net.staticstudios.data;

import net.staticstudios.data.misc.DataTest;
import net.staticstudios.data.mock.MockUser;
import org.junit.jupiter.api.Test;

public class SQLParseTest extends DataTest {

    @Test
    public void testParse() {
        DataManager dm = getMockEnvironments().getFirst().dataManager();
        dm.extractMetadata(MockUser.class);
        dm.getSQLBuilder().parse(MockUser.class).forEach(System.out::println);
    }
}