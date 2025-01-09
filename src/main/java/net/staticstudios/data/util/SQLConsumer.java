package net.staticstudios.data.util;

import java.sql.SQLException;

public interface SQLConsumer<T> {
    void accept(T connection) throws SQLException;
}
