package net.staticstudios.data.util;

import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.SQLException;

public interface ConnectionJedisConsumer {
    void accept(Connection connection, Jedis jedis) throws SQLException;
}
