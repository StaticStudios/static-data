package net.staticstudios.data.util;

import java.sql.Connection;
import java.sql.SQLException;

public interface ConnectionConsumer {
    void accept(Connection connection) throws SQLException;
}
