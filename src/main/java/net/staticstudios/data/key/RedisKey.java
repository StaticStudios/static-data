package net.staticstudios.data.key;

import net.staticstudios.data.data.value.redis.RedisValue;

import java.util.UUID;

public class RedisKey extends DataKey {
    private final String identifyingKey;
    private final String holderSchema;
    private final String holderTable;
    private final String holderIdColumn;
    private final UUID rootHolderId;

    public RedisKey(String holderSchema, String holderTable, String holderIdColumn, UUID rootHolderId, String identifyingKey) {
        super(holderSchema, holderTable, holderIdColumn, rootHolderId, identifyingKey);
        this.identifyingKey = identifyingKey;
        this.holderSchema = holderSchema;
        this.holderTable = holderTable;
        this.holderIdColumn = holderIdColumn;
        this.rootHolderId = rootHolderId;
    }

    public RedisKey(RedisValue<?> data) {
        this(
                data.getHolder().getRootHolder().getSchema(),
                data.getHolder().getRootHolder().getTable(),
                data.getHolder().getRootHolder().getIdentifier().getColumn(),
                data.getHolder().getRootHolder().getRootHolder().getId(),
                data.getIdentifyingKey()
        );
    }

    public static RedisKey fromString(String key) {
        String[] parts = key.split(":");
        return new RedisKey(parts[0], parts[1], parts[2], UUID.fromString(parts[3]), parts[4]);
    }

    /**
     * Omit the root holder id from the key
     *
     * @return the partial key
     */
    public String toPartialKey() {
        return String.format("%s:%s:%s:?:%s", holderSchema, holderTable, holderIdColumn, identifyingKey);
    }

    public String getIdentifyingKey() {
        return identifyingKey;
    }

    public String getHolderSchema() {
        return holderSchema;
    }

    public String getHolderTable() {
        return holderTable;
    }

    public String getHolderIdColumn() {
        return holderIdColumn;
    }

    public UUID getRootHolderId() {
        return rootHolderId;
    }

    @Override
    public String toString() {
        return String.format("%s:%s:%s:%s:%s", holderSchema, holderTable, holderIdColumn, rootHolderId, identifyingKey);
    }
}
