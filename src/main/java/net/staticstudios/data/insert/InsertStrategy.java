package net.staticstudios.data.insert;

public enum InsertStrategy {
    /**
     * Overwrite existing data with new data.
     */
    OVERWRITE_EXISTING,
    /**
     * Do not overwrite existing data, only insert if no data exists.
     */
    PREFER_EXISTING,
}
