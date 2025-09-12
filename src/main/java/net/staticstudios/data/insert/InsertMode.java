package net.staticstudios.data.insert;

public enum InsertMode {
    /**
     * Insert into the cache and database synchronously.
     * If either fails, neither will be updated.
     * This is inherently blocking.
     */
    SYNC,
    /**
     * Immediately inserts into the cache.
     * If the cached insert fails, then the database will not be updated.
     * If the database update fails however, the cache will NOT be reverted.
     */
    ASYNC
}
