package net.staticstudios.data;

import net.staticstudios.data.misc.DataTest;
import net.staticstudios.data.parse.DDLStatement;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DDLConcurrencyTest extends DataTest {

    @Test
    public void concurrentDDLIsSerializedAcrossDataManagers() throws Exception {
        DataManager first = getMockEnvironments().getFirst().dataManager();
        DataManager second = createMockEnvironment().dataManager();
        List<DDLStatement> ddl = List.of(
                DDLStatement.of("", "DROP TABLE IF EXISTS public.static_data_concurrent_ddl_test"),
                DDLStatement.of("", "SELECT pg_sleep(0.25)"),
                DDLStatement.of("", "CREATE TABLE public.static_data_concurrent_ddl_test (id INTEGER PRIMARY KEY)")
        );

        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<?> firstLoad = executor.submit(() -> runDDL(first, ddl, ready, start));
            Future<?> secondLoad = executor.submit(() -> runDDL(second, ddl, ready, start));

            assertTrue(ready.await(5, TimeUnit.SECONDS));
            start.countDown();
            firstLoad.get(10, TimeUnit.SECONDS);
            secondLoad.get(10, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
        }

        try (Statement statement = getConnection().createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT COUNT(*) FROM public.static_data_concurrent_ddl_test")) {
            resultSet.next();
            assertEquals(0, resultSet.getInt(1));
        }
    }

    private static void runDDL(DataManager dataManager, List<DDLStatement> ddl, CountDownLatch ready, CountDownLatch start) {
        try {
            ready.countDown();
            start.await();
            dataManager.getDataAccessor().runDDL(ddl);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
