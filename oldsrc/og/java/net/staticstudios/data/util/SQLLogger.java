package net.staticstudios.data.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class SQLLogger {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    protected void logSQL(String sql) {
        logger.debug("Executing SQL: {}", sql);
    }
}
