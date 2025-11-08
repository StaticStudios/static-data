package net.staticstudios.data;

import net.staticstudios.data.misc.DataTest;
import net.staticstudios.data.mock.user.MockUser;
import net.staticstudios.data.util.ColumnValuePair;
import net.staticstudios.data.util.ColumnValuePairs;
import net.staticstudios.data.util.RedisUtils;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class CachedValueTest extends DataTest {

    @Test
    public void testBasic() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(MockUser.class);
        MockUser user = MockUser.builder(dataManager)
                .id(UUID.randomUUID())
                .name("john doe")
                .insert(InsertMode.ASYNC);

        user.onCooldown.set(false);
        assertEquals(false, user.onCooldown.get());

        user.onCooldown.set(true);
        assertEquals(true, user.onCooldown.get());

        user.onCooldown.set(false);
        assertEquals(false, user.onCooldown.get());
    }

    @Test
    public void testFallback() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(MockUser.class);
        MockUser user = MockUser.builder(dataManager)
                .id(UUID.randomUUID())
                .name("john doe")
                .insert(InsertMode.ASYNC);

        assertEquals(false, user.onCooldown.get());
        assertEquals(0, user.cooldownUpdates.get());

        waitForDataPropagation();

        Jedis jedis = getJedis();

        String onCooldownKey = RedisUtils.buildRedisKey("public", "users", "on_cooldown", user.getIdColumns());
        String cooldownUpdatesKey = RedisUtils.buildRedisKey("public", "users", "cooldown_updates", user.getIdColumns());

        assertNull(jedis.get(onCooldownKey));
        assertNull(jedis.get(cooldownUpdatesKey));
    }

    @Test
    public void testUpdateHandler() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(MockUser.class);
        MockUser user = MockUser.builder(dataManager)
                .id(UUID.randomUUID())
                .name("john doe")
                .insert(InsertMode.ASYNC);

        waitForUpdateHandlers();
        assertEquals(0, user.cooldownUpdates.get());

        user.onCooldown.set(false);
        assertEquals(false, user.onCooldown.get());

        //the fallback value is false, so we didn't change anything. update handlers shouldn't be fired
        waitForUpdateHandlers();
        assertEquals(0, user.cooldownUpdates.get());


        user.onCooldown.set(true);
        assertEquals(true, user.onCooldown.get());

        waitForUpdateHandlers();
        assertEquals(1, user.cooldownUpdates.get());


        user.onCooldown.set(true);
        assertEquals(true, user.onCooldown.get());

        //didnt change, so no update
        waitForUpdateHandlers();
        assertEquals(1, user.cooldownUpdates.get());


        user.onCooldown.set(false);
        assertEquals(false, user.onCooldown.get());

        waitForUpdateHandlers();
        assertEquals(2, user.cooldownUpdates.get());
    }

    @Test
    public void testUpdateRedis() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(MockUser.class);
        MockUser user = MockUser.builder(dataManager)
                .id(UUID.randomUUID())
                .name("john doe")
                .insert(InsertMode.ASYNC);

        Jedis jedis = getJedis();

        String onCooldownKey = RedisUtils.buildRedisKey("public", "users", "on_cooldown", user.getIdColumns());
        String cooldownUpdatesKey = RedisUtils.buildRedisKey("public", "users", "cooldown_updates", user.getIdColumns());

        user.onCooldown.set(true);
        waitForUpdateHandlers();
        user.cooldownUpdates.set(1);
        waitForDataPropagation();
        assertEquals("true", jedis.get(onCooldownKey));
        assertEquals("1", jedis.get(cooldownUpdatesKey));

        user.onCooldown.set(null);
        waitForUpdateHandlers();
        user.cooldownUpdates.set(null);
        waitForDataPropagation();
        assertNull(jedis.get(onCooldownKey));
        assertNull(jedis.get(cooldownUpdatesKey));

        user.onCooldown.set(false); //fallback
        waitForUpdateHandlers();
        user.cooldownUpdates.set(0); //fallback
        waitForDataPropagation();
        assertNull(jedis.get(onCooldownKey));
        assertNull(jedis.get(cooldownUpdatesKey));
    }

    @Test
    public void testLoadCachedValues() {
        UUID userId = UUID.randomUUID();
        ColumnValuePairs columnValuePairs = new ColumnValuePairs(new ColumnValuePair("id", userId));

        Jedis jedis = getJedis();

        String onCooldownKey = RedisUtils.buildRedisKey("public", "users", "on_cooldown", columnValuePairs);
        String cooldownUpdatesKey = RedisUtils.buildRedisKey("public", "users", "cooldown_updates", columnValuePairs);

        jedis.set(onCooldownKey, "true");
        jedis.set(cooldownUpdatesKey, "5");

        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(MockUser.class);
        MockUser user = MockUser.builder(dataManager)
                .id(userId)
                .name("john doe")
                .insert(InsertMode.ASYNC);

        assertEquals(true, user.onCooldown.get());
        assertEquals(5, user.cooldownUpdates.get());
    }
}