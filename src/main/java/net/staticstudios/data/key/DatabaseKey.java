package net.staticstudios.data.key;

public abstract class DatabaseKey extends DataKey {
    public DatabaseKey(Object... parts) {
        super(parts);
    }

    public abstract String getSchema();

    public abstract String getTable();
}
